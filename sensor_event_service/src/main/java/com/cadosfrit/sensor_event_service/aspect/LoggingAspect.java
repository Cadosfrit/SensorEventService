package com.cadosfrit.sensor_event_service.aspect;

import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.stereotype.Component;
import org.springframework.util.StopWatch;

@Aspect
@Component
@Slf4j
public class LoggingAspect {

    /**
     * Intercepts all methods in the 'service' package (and sub-packages).
     */
    @Around("execution(* com.cadosfrit.sensor_event_service.service..*(..))")
    public Object logExecutionTime(ProceedingJoinPoint joinPoint) throws Throwable {
        MethodSignature signature = (MethodSignature) joinPoint.getSignature();
        String className = signature.getDeclaringType().getSimpleName();
        String methodName = signature.getName();

        final StopWatch stopWatch = new StopWatch();

        log.info("--> Entering {}.{} with arguments = {}", className, methodName, joinPoint.getArgs());

        try {
            stopWatch.start();
            Object result = joinPoint.proceed();
            stopWatch.stop();

            log.info("<-- Exited {}.{} :: Time taken = {} ms",
                    className, methodName, stopWatch.getTotalTimeMillis());
            return result;
        } catch (Throwable ex) {
            log.error("<!! Exception in {}.{} :: Message = {}", className, methodName, ex.getMessage());
            throw ex;
        }
    }
}
