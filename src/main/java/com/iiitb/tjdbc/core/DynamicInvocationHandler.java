package com.iiitb.tjdbc.core;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;

import static com.iiitb.tjdbc.util.CommonUtils.EXECUTE_QUERY;
import static com.iiitb.tjdbc.util.CommonUtils.EXECUTE_UPDATE;

public class DynamicInvocationHandler implements InvocationHandler {

    private final Object target;

    private final TStatement tStatement;

    public DynamicInvocationHandler(Object target) {
        this.target = target;
        this.tStatement = new TStatement();
    }

    @Override
    public Object invoke(Object o, Method method, Object[] objects) throws Throwable {
        if (method.getName().equals(EXECUTE_QUERY) || method.getName().equals(EXECUTE_UPDATE)) {
            objects[0] = tStatement.processQuery(objects[0].toString());
            System.out.println("OBJ[0] : " + objects[0]);
        }
        return method.invoke(target,objects);
    }
}
