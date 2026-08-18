/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024, 2025 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */
package org.monetdb.testinfra;

import org.junit.platform.engine.TestExecutionResult;
import org.junit.platform.launcher.*;
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder;
import org.junit.platform.launcher.core.LauncherFactory;
import org.junit.platform.launcher.listeners.SummaryGeneratingListener;
import org.junit.platform.launcher.listeners.TestExecutionSummary;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Optional;

import static org.junit.platform.engine.TestExecutionResult.Status.SUCCESSFUL;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectPackage;
import static org.junit.platform.launcher.TagFilter.includeTags;

/**
 * JUnit test launcher compatible with MonetDB's Mtest.py.
 *
 * This means it prefixes non-error output with '# ' and the {@link #run()} method
 * provides an exit code to return.
 */
public class MtestLauncher {
	private final PrintStream always;
	private final LauncherDiscoveryRequest discoveryRequest;
	private PrintStream verbose;
	private TestPlan testPlan;

	public MtestLauncher(String tags) {
		always = new PrintStream(new PrefixedOutputStream(System.out));
		verbose = new PrintStream(new DevNull());
		discoveryRequest = buildDiscoveryRequest(tags);
	}

	public void setVerbose(boolean enabled) {
		verbose = enabled ? always : new PrintStream(new DevNull());
	}

	public PrintStream logAlways() {
		return always;
	}
	public PrintStream logVerbose() {
		return verbose;
	}

	private static LauncherDiscoveryRequest buildDiscoveryRequest(String tags) {
		LauncherDiscoveryRequest request = LauncherDiscoveryRequestBuilder
				.request()
				.selectors(
						selectPackage("org.monetdb"),
						selectClass("JDBC_API_Tester"))
				.filters(includeTags(tags))
				.build();
		return request;
	}

	public int run() {
		try (LauncherSession sess = LauncherFactory.openSession()) {
			Launcher launcher = sess.getLauncher();
			testPlan = launcher.discover(discoveryRequest);

			SummaryGeneratingListener summaryListener = new SummaryGeneratingListener();
			MyListener verbosityListener = new MyListener();
			launcher.registerTestExecutionListeners(verbosityListener, summaryListener);


			verbose.println("==================== Start ====================================================");
			verbose.println();
			long t0 = System.currentTimeMillis();
			launcher.execute(testPlan);
			long t1 = System.currentTimeMillis();
			verbose.println();
			verbose.println("==================== Done =====================================================");
			always.println("Tests took " + (t1 - t0) + "ms");

			TestExecutionSummary summary = summaryListener.getSummary();
			summary.printTo(new PrintWriter(always));
			if (summary.getTotalFailureCount() > 0)
				return 1;
			else
				return 0;
		}
	}

	private static class DevNull extends OutputStream {
		@Override
		public void write(int i) throws IOException {
		}

		@Override
		public void write(byte[] b) throws IOException {}

		@Override
		public void write(byte[] b, int off, int len) throws IOException {}
	}

	private class MyListener implements TestExecutionListener {
		long started;

		StringBuffer nestedName = new StringBuffer();
		ArrayList<Integer> positionStack = new ArrayList<>();

		private void pushName(TestIdentifier identifier) {
			positionStack.add(nestedName.length());
			if (!testPlan.getRoots().contains(identifier)) {
				if (nestedName.length() > 0)
					nestedName.append(" / ");
				nestedName.append(identifier.getDisplayName());
			}
		}

		private void popName() {
			int pos = positionStack.remove(positionStack.size() - 1);
			nestedName.setLength(pos);
		}

		private boolean isLeaf(TestIdentifier identifier) {
			return testPlan.getChildren(identifier).isEmpty();
		}

		@Override
		public void executionSkipped(TestIdentifier identifier, String reason) {
			pushName(identifier);
			verbose.println("===== SKIP " + nestedName + ": " + reason);
			popName();
		}

		@Override
		public void executionStarted(TestIdentifier identifier) {
			pushName(identifier);

			if (isLeaf(identifier)) {
				verbose.println("***** RUN  " + nestedName);
				started = System.currentTimeMillis();
			}
		}

		@Override
		public void executionFinished(TestIdentifier identifier, TestExecutionResult result) {
			if (result.getStatus() != SUCCESSFUL || isLeaf(identifier))
				reportCompletion(identifier, result);
			popName();
		}

		private void reportCompletion(TestIdentifier testIdentifier, TestExecutionResult result) {
			TestExecutionResult.Status status = result.getStatus();
			Optional<Throwable> optException = result.getThrowable();
			long elapsedMillis = System.currentTimeMillis() - started;

			if (status == SUCCESSFUL && elapsedMillis < 40) {
				// the one line printed by executionStarted() is enough
				return;
			}
			switch (status) {
				case SUCCESSFUL:
					verbose.println("           ok after " + elapsedMillis + "ms");
					break;
				default:
					verbose.println("            " + status);
					System.err.println();
					System.err.println("!!! TEST " + nestedName + " FAILED");
					if (optException.isPresent()) {
						Throwable exception = optException.get();
						exception.printStackTrace(System.err);
					}
					System.err.println();
					System.err.println();
			}
		}
	}
}
