package de.tok.sql;

/**
 * Created by munline on 25/04/16.
 */
public abstract class LogicalElement {
    protected LogicalElement next = null;

    public void execute(){}

    public LogicalElement getNext() {
        return next;
    }

    public void setNext(LogicalElement next) {
        this.next = next;
    }
}
