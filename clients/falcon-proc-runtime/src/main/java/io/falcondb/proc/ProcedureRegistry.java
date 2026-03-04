package io.falcondb.proc;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ProcedureRegistry {
    private final Map<String, Procedure> procs = new ConcurrentHashMap<>();

    public void register(String name, Procedure proc) {
        procs.put(normalize(name), proc);
    }

    public Procedure get(String name) throws ProcException {
        Procedure p = procs.get(normalize(name));
        if (p == null) throw ProcException.notFound(name);
        return p;
    }

    public boolean contains(String name) {
        return procs.containsKey(normalize(name));
    }

    private String normalize(String name) {
        return name.toLowerCase().trim();
    }

    /** Scan and register all Procedure implementations in a package prefix (reflective). */
    public static ProcedureRegistry scanPackage(String packagePrefix) {
        ProcedureRegistry registry = new ProcedureRegistry();
        try {
            ClassLoader cl = Thread.currentThread().getContextClassLoader();
            String path = packagePrefix.replace('.', '/');
            java.net.URL url = cl.getResource(path);
            if (url == null) return registry;
            java.io.File dir = new java.io.File(url.toURI());
            if (!dir.isDirectory()) return registry;
            for (java.io.File f : dir.listFiles()) {
                if (!f.getName().endsWith(".class")) continue;
                String className = packagePrefix + "." + f.getName().replace(".class", "");
                try {
                    Class<?> cls = Class.forName(className);
                    if (Procedure.class.isAssignableFrom(cls) && !cls.isInterface()) {
                        Procedure inst = (Procedure) cls.getDeclaredConstructor().newInstance();
                        String procName = cls.getSimpleName().replaceFirst("^Proc_", "");
                        registry.register(procName, inst);
                    }
                } catch (Exception ignored) {}
            }
        } catch (Exception ignored) {}
        return registry;
    }
}
