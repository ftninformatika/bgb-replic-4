package bisis.records.serializers;

import java.util.Iterator;

import bisis.format.PubTypes;
import bisis.records.Field;
import bisis.records.Record;
import bisis.records.Subfield;
import bisis.utils.Signature;
import bisis.utils.StringUtils;


/**
 * 
 * @author mbranko@uns.ns.ac.yu
 */
public class IsisSerializer {

  public static String toISISFormat(Record record) {
    StringBuffer buff = new StringBuffer(1024);
    buff.append("! ID ");
    buff.append(StringUtils.padChars(Integer.toString(record.getRecordID()), 
        '0', 6));
    buff.append('\n');
    Iterator<Field> it = record.getFields().iterator();
    while (it.hasNext()) {
      Field f = it.next();
      if (PubTypes.getFormat().containsSecondaryFields(f.getName()))
        continue;
      buff.append("!v");
      buff.append(f.getName());
      buff.append('!');
      Iterator<Subfield> it2 = f.getSubfields().iterator(); 
      while (it2.hasNext()) {
        Subfield sf = it2.next();
        buff.append('^');
        buff.append(sf.getName());
        if ((f.getName().equals("996") || f.getName().equals("997")) && sf.getName() == 'd')
          buff.append(Signature.userDisplay(sf.getContent()));
        else
          buff.append(sf.getContent());
      }
      buff.append('\n');
    }
    return buff.toString();
  }
  
}
