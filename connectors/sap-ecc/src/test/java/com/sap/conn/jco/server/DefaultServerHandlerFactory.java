package com.sap.conn.jco.server;

import java.util.HashMap;
import java.util.Map;

public final class DefaultServerHandlerFactory {
    private DefaultServerHandlerFactory() {}

    public static class FunctionHandlerFactory implements JCoServerFunctionHandlerFactory {
        private final Map<String, JCoServerFunctionHandler> handlers = new HashMap<>();

        public FunctionHandlerFactory() {}

        public void registerHandler(String functionName, JCoServerFunctionHandler handler) {
            handlers.put(functionName, handler);
        }

        @Override
        public JCoServerFunctionHandler getCallHandler(JCoServerContext context, String functionName) {
            return handlers.get(functionName);
        }
    }
}
