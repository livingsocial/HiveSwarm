package com.livingsocial.hive.udf;

import com.livingsocial.hive.Utils;

import org.apache.hadoop.hive.ql.udf.UDAFMax;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.hive.ql.exec.description;
import org.apache.hadoop.io.Text;


@description(
    name = "smax",
    value = "_FUNC_(expr) - Returns the maximum value of expr, treating strings as dates"
    )
public class SMax extends UDAFMax {
  static public class SMaxStringEvaluator implements UDAFEvaluator {
    private Text mMax;
    private boolean mEmpty;

    public SMaxStringEvaluator() {
      super();
      init();
    }

    public void init() {
      mMax = null;
      mEmpty = true;
    }

    public boolean iterate(Text o) {
      if (o != null) {
        if (mEmpty) {
          mMax = new Text(o);
          mEmpty = false;
        } else if (Utils.stringToTimestamp(mMax.toString()) < Utils.stringToTimestamp(o.toString())) {
          mMax.set(o);
        }
      }
      return true;
    }

    public Text terminatePartial() {
      return mEmpty ? null : mMax;
    }

    public boolean merge(Text o) {
      return iterate(o);
    }

    public Text terminate() {
      return mEmpty ? null : mMax;
    }
  }
}
