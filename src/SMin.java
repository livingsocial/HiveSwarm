package com.livingsocial.hive.udf;

import com.livingsocial.hive.Utils;

import org.apache.hadoop.hive.ql.udf.UDAFMin;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.hive.ql.exec.description;
import org.apache.hadoop.io.Text;


@description(
    name = "smin",
    value = "_FUNC_(expr) - Returns the minimum value of expr, treating strings as dates"
    )
public class SMin extends UDAFMin {
  static public class SMinStringEvaluator implements UDAFEvaluator {
    private Text mMin;
    private boolean mEmpty;

    public SMinStringEvaluator() {
      super();
      init();
    }

    public void init() {
      mMin = null;
      mEmpty = true;
    }

    public boolean iterate(Text o) {
      if (o != null) {
        if (mEmpty) {
          mMin = new Text(o);
          mEmpty = false;
        } else if (Utils.stringToTimestamp(mMin.toString()) > Utils.stringToTimestamp(o.toString())) {
          mMin.set(o);
        }
      }
      return true;
    }

    public Text terminatePartial() {
      return mEmpty ? null : mMin;
    }

    public boolean merge(Text o) {
      return iterate(o);
    }

    public Text terminate() {
      return mEmpty ? null : mMin;
    }
  }
}
