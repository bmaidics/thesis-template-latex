/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.common;

import com.google.common.collect.Interner;
import com.google.common.collect.Interners;
import org.apache.commons.lang3.NotImplementedException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.Map;
import java.util.Iterator;
import java.util.Collections;
import java.util.Collection;
import java.io.Reader;
import java.io.PrintWriter;
import java.io.OutputStream;
import java.io.IOException;

/**
 * Implementation of Properties to save memory.
 * When multiple HiveConf objects are created. Most of the time these
 * Configuration object have the same base and each session add their own
 * Properties to it. With this implementation we can intern the base
 * to prevent this overhead.
 */
public class HiveConfProperties extends Properties {

  private Properties interned;

  /**
   * We can't allow removing from the interned Properties.
   * So to be able to give a correct answer when getting a Property,
   * we should store the Properties, and use this for getProperty and size functions.
   * Based on how we use HiveConfs, it will only store a couple of values.
   */
  private Properties removed;

  //Used for calculating size.
  private int duplicatedPropertiesCount;

  private static Interner<Properties> interner = Interners.newWeakInterner();

  public HiveConfProperties(Properties p) {
    if(p != null) {
      interned = interner.intern(p);
    }
    removed =  new Properties();
    duplicatedPropertiesCount=0;
  }

  /**
   * Merging the interned and non-interned Properties object.
   * If a property was previously removed, don't add it to the merged.
   */
  private Properties mergeProperties() {
    Properties properties = new Properties();
    if(interned != null) {
      Iterator it = interned.entrySet().iterator();
      while (it.hasNext()) {
        Map.Entry pair = (Map.Entry)it.next();
        if(!removed.containsKey(pair.getKey())) {
          properties.setProperty((String) pair.getKey(), (String) pair.getValue());
        }
      }
    }
    Iterator it = super.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry pair = (Map.Entry)it.next();
      properties.setProperty((String) pair.getKey(), (String) pair.getValue());
    }
    return properties;
  }

  /*************   Public API of java.util.Properties   ************/

  /**
   * If non-interned (super) contains return that value.
   * if not return the value from the base (interned), if it is not null.
   */
  @Override
  public String getProperty(String key) {
    String property = super.getProperty(key);
    if (property == null) {
      //If it is not in the removed, then the Property is still valid (not removed).
      if(interned != null && !removed.containsKey(key)) {
        property = interned.getProperty(key);
      }
    }
    return property;
  }

  /**
   * If non-interned (super) contains, return that value.
   * - we can't use the super's getProperty(key, defaulValue)
   * otherwise it'd return the defaultValue instead of the value from interned -
   * if not return the value from the base (interned), if it is not null.
   */
  @Override
  public String getProperty(String key, String defaultValue) {
    String property = super.getProperty(key);
    if (property == null) {
      //If it is not in the removed, then the Property is still valid (not removed).
      if(interned != null && !removed.containsKey(key)) {
        property = interned.getProperty(key, defaultValue);
      }
    }
    return property != null ? property : defaultValue;
  }

  /**
   * Setting the Property in non-interned (super).
   * Duplicates can happen this way: if interned already contains a property
   * that is being added in the non-interned, there will be 2 properties with the same key
   * However this will not be a problem, because of the way getProperty works
   * (We cannot remove it from interned, since other HiveConfs use the same.
   */
  @Override
  public synchronized Object setProperty(String key, String value) {
    if(interned != null && interned.containsKey(key) && !super.containsKey(key)){
      String internedValue = interned.getProperty(key);
      if(internedValue.equals(value)) {
        return internedValue;
      }
      duplicatedPropertiesCount++;
    }
    //If removed contains this key, and we want to set it, it is no longer "removed"
    if(removed.containsKey(key)) {
      removed.remove(key);
    }
    return super.setProperty(key, value);
  }

  /**
   * Merge the keys of the two parts (interned, non-interned) and return.
   */
  @Override
  public Set<String> stringPropertyNames() {
    return mergeProperties().stringPropertyNames();
  }

  /**
   * The same as HashTable's keys function.
   */
  @Override
  public Enumeration<?> propertyNames() {
    return keys();
  }

  /*************   Not used java.util.Properties functions in Configuration class.  ************/

  @Override
  public void list(PrintStream out) {
    throw new NotImplementedException("HiveConfProperties.list not implemented");
  }

  @Override
  public void list(PrintWriter out) {
    throw new NotImplementedException("HiveConfProperties.list not implemented");
  }

  @Override
  public synchronized void load(InputStream inStream) throws IOException {
    throw new NotImplementedException("HiveConfProperties.load not implemented");
  }

  @Override
  public synchronized void load(Reader reader) throws IOException {
    throw new NotImplementedException("HiveConfProperties.load not implemented");
  }

  @Override
  public synchronized void loadFromXML(InputStream inStream) throws IOException {
    throw new NotImplementedException("HiveConfProperties.loadFromXML not implemented");
  }

  @Override
  public void store(OutputStream out, String comments) throws IOException {
    throw new NotImplementedException("HiveConfProperties.store not implemented");
  }

  @Override
  public void storeToXML(OutputStream os, String comment) throws IOException {
    throw new NotImplementedException("HiveConfProperties.storeToXML not implemented");
  }

  @Override
  public void storeToXML(OutputStream os, String comment, String encoding)
      throws IOException {
    throw new NotImplementedException("HiveConfProperties.storeToXML not implemented");
  }

  /*************   Public API of java.util.Hashtable   ************/

  /**
   * Size of HiveConfProperties is the size of interned + size of non-interned - duplicatedPropertiesCount.
   * We have to subtract the duplicatedPropertiesCount, because duplicates can happen:
   * If we set a property which is already in the interned, we have a duplicate.
   * Also we have to subtract the size of the removed Properties, since those had been removed
   */
  @Override
  public synchronized int size() {
    if(interned != null) {
      return super.size() + interned.size() - duplicatedPropertiesCount - removed.size();
    }
    return super.size();
  }

  /**
   * If non-interned (super) contains return that value.
   * If not return the value from the base (interned), if it is not null.
   */
  @Override
  public synchronized Object get(Object key) {
    Object o = super.get(key);
    if (o == null) {
      if(interned != null && !removed.containsKey(key)) {
        o = interned.get(key);
      }
    }
    return o;
  }

  /**
   * If non-interned (super) contains return that value.
   * - we can't use the super's get(key, defaultValue)
   * otherwise it'd return the defaultValue instead of the value from interned -
   * If not return the value from the base (interned), if it is not null.
   */
  @Override
  public synchronized Object getOrDefault(Object key, Object defaultValue) {
    Object property = super.get(key);
    if (property == null) {
      if(interned != null && !removed.containsKey(key)) {
        property = interned.getOrDefault(key, defaultValue);
      }
    }
    return property != null ? property : defaultValue;
  }

  /**
   * Merge the two Properties (interned, non-interned) with putAll and return the merged Properties.
   */
  @Override
  public synchronized Object clone() {
    return mergeProperties().clone();
  }

  /**
   * Return true if non-interned or interned contains the key.
   */
  @Override
  public synchronized boolean containsKey(Object key) {
    if(interned != null) {
      return !removed.containsKey(key) && (super.containsKey(key) || interned.containsKey(key));
    }
    return super.containsKey(key);
  }

  /**
   * Return true if non-interned or interned contains the value.
   */
  @Override
  public synchronized boolean containsValue(Object value) {
    if(interned != null) {
      return !removed.containsValue(value) && (super.containsValue(value) || interned.containsValue(value));
    }
    return super.containsValue(value);
  }

  @Override
  public synchronized int hashCode() {
    if(interned != null) {
      return super.hashCode() & interned.hashCode();
    }
    return super.hashCode();
  }

  /**
   * Implementation of equals follows the logic of the equals implementation in HashTable.
   * But we need to divide into two parts: when other is HiveConfProperties and when not.
   * If other is HiveConfProperties -> need to check the interned for equality as well.
   */
  @Override
  public synchronized boolean equals(Object o) {
    if (o == this) {
      return true;
    }

    if (!(o instanceof Map)) {
      return false;
    }
    Map<Object, Object> t = (Map<Object, Object>) o;
    //If other is HiveConfProperties we need to check the interned Properties as well.
    if (t instanceof HiveConfProperties) {
      HiveConfProperties otherHiveConfProperties = (HiveConfProperties) t;
      //If sizes do not match => they are not equals
      if (otherHiveConfProperties.size() != this.size()) {
        return false;
      }
      try {
        Iterator<Map.Entry<Object, Object>> i = super.entrySet().iterator();
        while (i.hasNext()) {
          Map.Entry<Object, Object> e = i.next();
          String key = (String) e.getKey();
          String value = (String) e.getValue();
          //If value is null "other" Properties shouldn't contain key
          if (value == null) {
            //Note that HiveConfProperties.get will be called so interned is counted
            if (!(otherHiveConfProperties.get(key) == null && otherHiveConfProperties.containsKey(key))) {
              return false;
            }
          } else {
            //If value is not null, value from this should be equal to value from other
            if (!value.equals(otherHiveConfProperties.get(key))) {
              return false;
            }
          }
        }
        //Going through this.interned Properties as well
        if(interned != null) {
          i = interned.entrySet().iterator();
          while (i.hasNext()) {
            Map.Entry<Object, Object> e = i.next();
            String key = (String) e.getKey();
            String value = (String) e.getValue();
            if (value == null) {
              //If value is null other Properties shouldn't contain key
              if (!(otherHiveConfProperties.get(key) == null && otherHiveConfProperties.containsKey(key))) {
                //We don't need to store in equals, we can instantly return false, since they can't be equal
                return false;
              }
            } else {
              if (!value.equals(otherHiveConfProperties.get(key))) {
                return false;
              }
            }
          }
        }
      } catch (ClassCastException unused) {
        return false;
      } catch (NullPointerException unused) {
        return false;
      }
      //If we didn't return false until this point => this and other are equals
      return true;
    } else {
      //other is not HiveConfProperties => not divided into 2 parts
      if (t.size() != this.size()) {
        return false;
      }
      try {
        //Iterating through other instead of this => no need to merge non-interned and interned Properties
        Iterator<Map.Entry<Object, Object>> i = t.entrySet().iterator();
        while (i.hasNext()) {
          Map.Entry<Object, Object> e = i.next();
          String key = (String) e.getKey();
          String value = (String) e.getValue();
          if (value == null) {
            if (!(this.get(key) == null && this.containsKey(key))) {
              return false;
            }
          } else {
            if (!value.equals(this.get(key))) {
              return false;
            }
          }
        }
      } catch (ClassCastException unused) {
        return false;
      } catch (NullPointerException unused) {
        return false;
      }
      return true;
    }

  }

  /**
   * Iterating through both interned and non-interned Properties and collect keys.
   */
  @Override
  public synchronized Enumeration<Object> keys() {
    return mergeProperties().keys();
  }

  /**
   * Merging interned and non-interned keys into a HashSet.
   */
  @Override
  public Set<Object> keySet() {
    return mergeProperties().keySet();
  }

  /**f
   * Merging interned and non-interned entries into a HashSet.
   */
  @Override
  public Set<Map.Entry<Object, Object>> entrySet() {
    return mergeProperties().entrySet();
  }

  /**
   * Putting the Property in non-interned (super).
   * Duplicates can happen this way: if interned already contains a property
   * that is being added in the non-interned, there will be 2 properties with the same key
   * However this will not be a problem, because of the way getProperty works
   * (We cannot remove it from interned, since other HiveConfs use the same.
   */
  @Override
  public synchronized Object put(Object key, Object value) {
    return super.put(key, value);
  }

  /**
   * Putting the properties in non-interned (super).
   * Duplicates can happen this way: if interned already contains some properties
   * that are being added in the non-interned, there will be duplicate with the same key
   * However this will not be a problem, because of the way getProperty works
   * (We cannot remove it from interned, since other HiveConfs use the same
   */
  @Override
  public synchronized void putAll(Map<? extends Object, ? extends Object> t) {
    super.putAll(t);
  }

  /**
   * We can't remove it from the interned Properties (other HiveConf-s may use it).
   * As a solution we can invalidate the property that we want to remove by putting it
   * to another Properties object (removed). If we want to get a property by its key
   * first we check if it's valid (not in the removed table).
   */
  @Override
  public synchronized Object remove(Object key) {
    if(interned != null) {
      if (interned.containsKey(key)) {
        //We can't remove from interned
        String v = interned.getProperty((String) key);
        removed.setProperty((String) key, v);
        return v;
      }
    }
    return super.remove(key);
  }

  /**
   * We can't remove it from the interned Properties (other HiveConf-s may use it).
   * As a solution we can invalidate the property that we want to remove by putting it
   * to another Properties object (removed). If we want to get a property by its key
   * first we check if it's valid (not in the removed table).
   */
  @Override
  public synchronized boolean remove(Object key, Object value) {
    if(interned != null) {
      if (interned.containsKey(key)) {
        //We can't remove from interned
        removed.setProperty((String) key, (String) value);
        return true;
      }
    }
    return super.remove(key, value);
  }

  /**
   * If we want to clear, we can't empty the interned Properties (other HiveConf's may use it).
   * so we just set it to null.
   */
  @Override
  public synchronized void clear() {
    super.clear();
    removed.clear();
    interned = null;
    duplicatedPropertiesCount=0;
  }

  /**
   * Merge the interned and non-interned using mergeProperties method and call
   * toString on the merged Properties. toString is only needed by tests
   */
  @Override
  public synchronized String toString() {
    return mergeProperties().toString();
  }

  @Override
  public synchronized boolean isEmpty() {
    return mergeProperties().isEmpty();
  }

  /*************   Not used java.util.HashTable functions in Configuration class.  ************/

  @Override
  public synchronized boolean contains(Object value) {
    throw new NotImplementedException("HiveConfProperties.contains not implemented");
  }

  @Override
  public synchronized Object compute(Object key, BiFunction remappingFunction) {
    throw new NotImplementedException("HiveConfProperties.compute not implemented");
  }

  @Override
  public synchronized Object computeIfAbsent(Object key, Function mappingFunction) {
    throw new NotImplementedException("HiveConfProperties.computeIfAbsent not implemented");
  }

  @Override
  public synchronized Object computeIfPresent(Object key, BiFunction remappingFunction) {
    throw new NotImplementedException("HiveConfProperties.computeIfPresent not implemented");
  }

  @Override
  public synchronized Enumeration<Object> elements() {
    throw new NotImplementedException("HiveConfProperties.elements not implemented");
  }

  @Override
  public synchronized void forEach(BiConsumer action) {
    throw new NotImplementedException("HiveConfProperties.foreach not implemented");
  }

  @Override
  public synchronized Object merge(Object key, Object value, BiFunction remappingFunction) {
    throw new NotImplementedException("HiveConfProperties.merge not implemented");
  }

  @Override
  public synchronized Object putIfAbsent(Object key, Object value) {
    throw new NotImplementedException("HiveConfProperties.putIfAbsent not implemented");
  }

  @Override
  public synchronized Object replace(Object key, Object value) {
    throw new NotImplementedException("HiveConfProperties.replace not implemented");
  }

  @Override
  public synchronized boolean replace(Object key, Object oldValue, Object newValue) {
    throw new NotImplementedException("HiveConfProperties.replace not implemented");
  }

  @Override
  public synchronized void replaceAll(BiFunction function) {
    throw new NotImplementedException("HiveConfProperties.replaceAll not implemented");
  }

  @Override
  public Collection<Object> values() {
    throw new NotImplementedException("HiveConfProperties.values not implemented");
  }
}

