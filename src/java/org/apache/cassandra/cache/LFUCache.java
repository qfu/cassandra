package org.apache.cassandra.cache;

/**
 * Created by fuyinlin on 5/30/17.
 *
 */

import java.util.*;

public class LFUCache<K,V> {
    class KeyNode<K,V> {
        K key;
        V val;
        int freq;
        public KeyNode (K key, V val) {
            this.key = key;
            this.val = val;
            this.freq = 1;
        }
    }


    class FreqNode {
        int freq;
        FreqNode prev;
        FreqNode next;
        Set<KeyNode> set; // keep the insertion order
        public FreqNode (int freq, FreqNode prev, FreqNode next) {
            this.freq = freq;
            this.prev = prev;
            this.next = next;
            set = new LinkedHashSet<>();
        }
    }


    long capacity;
    int size;
    Map<K, KeyNode> keyMap;
    Map<Integer, FreqNode> freqMap;

    FreqNode head;


    //Constructor
    public LFUCache(long capacity) {
        head = null;
        keyMap = new HashMap<>();
        freqMap = new HashMap<>();
        this.capacity = capacity;
    }

    public V get(K key) {
        if (keyMap.containsKey(key)) {
            KeyNode keyNode = keyMap.get(key);
            V val = (V)keyNode.val;
            increase(key, val);
            return val;
        }
        return null;
    }

    public V put(K key, V val) {
        if (this.capacity == 0) return null;
        if (keyMap.containsKey(key)) {
            V value = (V)keyMap.get(key).val;
            increase(key, val);
            return value;
        }
        if (keyMap.size() == this.capacity) {
            removeKeyNode(head);
            size--;
        }
        size++;
        insertKeyNode(key, val);
        return null;
    }

    // helper function
    // increase freq of key, update val if necessary - yes
    public void increase(K key, V val) {
        KeyNode keynode = keyMap.get(key);
        // update val
        keynode.val = val;
        FreqNode freqnode = freqMap.get(keynode.freq);
        keynode.freq += 1;
        FreqNode nextFreqNode = freqnode.next;
        if (nextFreqNode == null) {
            nextFreqNode = new FreqNode(keynode.freq, freqnode, null);
            freqnode.next = nextFreqNode;
            // new frequency, new node
            freqMap.put(keynode.freq, nextFreqNode);
        }
        if (nextFreqNode != null && nextFreqNode.freq > keynode.freq) {

            nextFreqNode = insertFreqNodePlus1(keynode.freq, freqnode);
        }
        unlinkKey(keynode, freqnode);
        linkKey(keynode, nextFreqNode);
    }
    // remove the head's oldest node - yes
    public void removeKeyNode(FreqNode fnode) {
        KeyNode knode = fnode.set.iterator().next();
        unlinkKey(knode, freqMap.get(knode.freq));
        keyMap.remove(knode.key);
    }
    // Inserts a new KeyNode<key, value> with freq 1. - yes
    public void insertKeyNode(K key, V val) {
        KeyNode keynode = new KeyNode(key, val);
        keyMap.put(key, keynode);
        if (!freqMap.containsKey(1)) {
            FreqNode freqnode = new FreqNode(1, null, head);
            freqnode.next = head;
            if (head != null)   head.prev = freqnode;
            head = freqnode;
            freqMap.put(1, freqnode);
        }
        linkKey(keynode, freqMap.get(1));
    }
    // insert a new freqnode with new freq after given "freqnode" - yes
    public FreqNode insertFreqNodePlus1(int freq, FreqNode freqnode) {
        FreqNode newfnode = new FreqNode(freq, freqnode, freqnode.next);
        freqMap.put(freq, newfnode);
        if (freqnode.next != null)  freqnode.next.prev = newfnode;
        freqnode.next = newfnode;
        return newfnode;
    }
    // Unlink keyNode from freqNode - yes
    public void unlinkKey(KeyNode keynode, FreqNode freqnode) {
        freqnode.set.remove(keynode);
        if (freqnode.set == null || freqnode.set.size() == 0)     deleteFreqNode(freqnode);
    }
    // Link keyNode to freqNode - yes
    public void linkKey(KeyNode keynode, FreqNode freqnode) {
        freqnode.set.add(keynode);
    }
    // delete freqnode if there is no appending keynode under this freq - yes
    public void deleteFreqNode(FreqNode freqnode) {
        FreqNode prev = freqnode.prev, next = freqnode.next;
        if (prev != null)   prev.next = next;
        if (next != null)   next.prev = prev;
        if (head == freqnode)   head = next;
        freqMap.remove(freqnode.freq);
    }



    public long capacity(){
        return capacity;
    }

    public void setCapacity(long capacity){
        this.capacity = capacity;
    }


    public V putIfAbsent(K key, V value){
        return this.put(key,value);
    }

    public boolean replace(K key, V old, V value){
        this.put(key,value);
        return true;
    }


    public V remove(K key){
        if(!keyMap.containsKey(key)) return null;
        KeyNode node = keyMap.get(key);
        V value = (V)node.val;
        int freq = node.freq;
        FreqNode freNode = freqMap.get(freq);
        freNode.set.remove(node);
        if(freNode.set.size() == 0)
            removeKeyNode(freNode);
        return value;
    }

    public int size(){
        return size;
    }

    public long weightedSize(){
        return 0;
    }
    public boolean isEmpty(){
        return size == 0;
    }

    public void clear(){

    }

    public Iterator<K> keyIterator(){
        return null;
    }

    public Iterator<K> hotKeyIterator(int n){
        return null;
    }

    public boolean containsKey(K key){
        return keyMap.containsKey(key);
    }
}