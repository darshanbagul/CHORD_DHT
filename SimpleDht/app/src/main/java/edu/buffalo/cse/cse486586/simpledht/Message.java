package edu.buffalo.cse.cse486586.simpledht;
import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;
/**
 * Created by darshanbagul on 08/04/17.
 */

public class Message implements Serializable {
    public ConcurrentHashMap<String, String> map = null;
    public String key;
    public String value;
    public int prevNode;
    public int nextNode;
    public MessageType operation;

    public Message(ConcurrentHashMap<String, String> map, String key, String value, int prevNode, int nextNode, MessageType operation) {
        this.map = map;
        this.key = key;
        this.value = value;
        this.prevNode = prevNode;
        this.nextNode = nextNode;
        this.operation = operation;
    }
}