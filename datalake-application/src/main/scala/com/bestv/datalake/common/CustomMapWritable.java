package com.bestv.datalake.common;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.io.AbstractMapWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

/**
 * Package: com.bestv.datalake.common.
 * User: SichengWang
 * Date: 2017/6/29
 * Time: 16:35
 * Project: DataLake
 */

@Public
@Stable
public class CustomMapWritable extends AbstractMapWritable implements Map<Text, Text> {
    private Map<Text, Text> instance;

    public CustomMapWritable() {
        this.instance = new HashMap();
    }

    public CustomMapWritable(CustomMapWritable other) {
        this();
        this.copy(other);
    }

    public void clear() {
        this.instance.clear();
    }

    public boolean containsKey(Object key) {
        return this.instance.containsKey(key);
    }

    public boolean containsValue(Object value) {
        return this.instance.containsValue(value);
    }

    public Set<Entry<Text, Text>> entrySet() {
        return this.instance.entrySet();
    }

    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else if (obj instanceof MapWritable) {
            MapWritable map = (MapWritable) obj;
            return this.size() != map.size() ? false : this.entrySet().equals(map.entrySet());
        } else {
            return false;
        }
    }

    public Text get(Object key) {
        return (Text) this.instance.get(key);
    }

    public int hashCode() {
        return 1 + this.instance.hashCode();
    }

    public boolean isEmpty() {
        return this.instance.isEmpty();
    }

    public Set<Text> keySet() {
        return this.instance.keySet();
    }

    public Text put(Text key, Text value) {
        this.addToMap(key.getClass());
        this.addToMap(value.getClass());
        return (Text) this.instance.put(key, value);
    }
    public Text remove(Object key) {
        return (Text) this.instance.remove(key);
    }

    @Override
    public void putAll(Map<? extends Text, ? extends Text> m) {
        Iterator i$ = m.entrySet().iterator();
        while (i$.hasNext()) {
            Entry e = (Entry) i$.next();
            this.put((Text) e.getKey(), (Text) e.getValue());
        }
    }

    public int size() {
        return this.instance.size();
    }

    public Collection<Text> values() {
        return this.instance.values();
    }

    public void write(DataOutput out) throws IOException {
        super.write(out);
        out.writeInt(this.instance.size());
        Iterator i$ = this.instance.entrySet().iterator();

        while (i$.hasNext()) {
            Entry e = (Entry) i$.next();
            out.writeByte(this.getId(((Writable) e.getKey()).getClass()));
            ((Writable) e.getKey()).write(out);
            out.writeByte(this.getId(((Writable) e.getValue()).getClass()));
            ((Writable) e.getValue()).write(out);
        }

    }

    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        this.instance.clear();
        int entries = in.readInt();

        for (int i = 0; i < entries; ++i) {
            Text key = (Text) ReflectionUtils.newInstance(this.getClass(in.readByte()), this.getConf());
            key.readFields(in);
            Text value = (Text) ReflectionUtils.newInstance(this.getClass(in.readByte()), this.getConf());
            value.readFields(in);
            this.instance.put(key, value);
        }

    }
}