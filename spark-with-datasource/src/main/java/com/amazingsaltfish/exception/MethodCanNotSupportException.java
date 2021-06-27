package com.amazingsaltfish.exception;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;



/**
 * @author: wmh
 * Create Time: 2021/6/24 11:01
 *   useage
 *     throw new MethodCanNotSupportException(this.getClass().getName(), Thread.currentThread().getStackTrace()[1].getMethodName());
 */
@Setter
@Getter
@AllArgsConstructor
public class MethodCanNotSupportException extends RuntimeException{
    private String className;
    private String methodName;

    @Override
    public String getMessage() {
        return String.format("This class: [%s] method name: [%s] haven't support this method yet!!!", className, methodName);
    }
}
