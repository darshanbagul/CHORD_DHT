package edu.buffalo.cse.cse486586.simpledht;

/**
 * Created by darshanbagul on 08/04/17.
 */

/*
 * This is a class that contains the different types of Message types. We shall be using these
 * types to decide the operation when each message is received at a node.
 */
public enum MessageType {
    JOIN,
    ACCEPT,
    INSERT,
    QUERY,
    FETCH_ALL,
    DELETE,
    SET_NEXT_NODE,
    NULL
}
