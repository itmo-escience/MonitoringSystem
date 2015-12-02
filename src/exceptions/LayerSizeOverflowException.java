/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package exceptions;

/**
 *
 * @author user
 */
public class LayerSizeOverflowException extends Exception {
    public LayerSizeOverflowException()
    {
        super("Data size extends data layer capacity.");
    }
 
    public LayerSizeOverflowException(String message)
    {
        super(message);
    }

    public LayerSizeOverflowException(Throwable cause)
    {
        super(cause);
    }

    public LayerSizeOverflowException(String message, Throwable cause)
    {
        super(message, cause);
    }

}
