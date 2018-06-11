package bisis.prefixes;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import bisis.records.Field;
import bisis.records.Godina;
import bisis.records.Primerak;
import bisis.records.Record;
import bisis.records.Subfield;
import bisis.records.Sveska;
import bisis.records.serializers.PrimerakSerializer;

/**
 * 
 * @author mbranko@uns.ns.ac.yu
 */
public class PrefixConverter {

  public static List<PrefixValue> toPrefixes(Record rec) {
    List<PrefixValue> retVal = new ArrayList<PrefixValue>();
    for (int i = 0; i < rec.getFieldCount(); i++) {
      Field field = rec.getField(i);
      fieldToPrefixes(retVal, field);
    }
    List<Primerak> primerci = rec.getPrimerci();
    List<Godina> godine = rec.getGodine();
    if (primerci != null) {
      for (int i = 0; i < primerci.size(); i++) {
        Primerak p = primerci.get(i);
        Field f = PrimerakSerializer.getField(p);
        fieldToPrefixes(retVal, f);
        indeksirajPrimerak(retVal, p);
      }

    }
    if (godine != null) {
      for (int i = 0; i < godine.size(); i++) {
        Godina g = godine.get(i);
        Field f = PrimerakSerializer.getField(g);
        fieldToPrefixes(retVal, f);
        indeksirajGodinu(retVal, g);
        List<Sveska> sveske = g.getSveske();
        if (sveske != null) {
          for (int j = 0; j < sveske.size(); j++) {
            Sveska s = sveske.get(j);
            indeksirajSvesku(retVal, s);
          }

        }
      }
    }

    return retVal;
  }

  private static void indeksirajGodinu(List<PrefixValue> dest, Godina g) {
    if (g.getInventator() != null)
      dest.add(new PrefixValue("BI", g.getInventator()));
  }

  private static void indeksirajPrimerak(List<PrefixValue> dest, Primerak p) {
    if (p.getInventator() != null)
      dest.add(new PrefixValue("BI", p.getInventator()));
    if (p.getDatumRacuna() != null)
      dest.add(new PrefixValue("DB", dateFormat.format(p.getDatumRacuna())));
  }

  private static void indeksirajSvesku(List<PrefixValue> dest, Sveska s) {
    dest.add(new PrefixValue("IN", s.getInvBroj()));
    if (s.getInventator() != null)
      dest.add(new PrefixValue("BI", s.getInventator()));

  }

  public static List<PrefixValue> getPrefixValues(Record rec, String prefName) {
    List<PrefixValue> retVal = toPrefixes(rec);
    Iterator<PrefixValue> it = retVal.iterator();
    while (it.hasNext()) {
      PrefixValue pv = it.next();
      if (!pv.prefName.equals(prefName))
        it.remove();
    }
    return retVal;
  }

  private static void fieldToPrefixes(List<PrefixValue> dest, Field field) {
    if (field.getName().startsWith("70") || field.getName().startsWith("90")) {
      String[] autor = prefixHandler.createAuthor(field);
      if (autor.length != 0) {
        if (!autor[0].equals(""))
          dest.add(new PrefixValue("AU", autor[0]));
        if (!autor[1].equals(""))
          dest.add(new PrefixValue("AU", autor[1]));
      }
    }
    if (field.getName().startsWith("60")) {
      String[] pnautor = prefixHandler.createAuthor(field);
      if (pnautor.length != 0) {
        if (!pnautor[0].equals(""))
          dest.add(new PrefixValue("PN", pnautor[0]));
        if (!pnautor[1].equals(""))
          dest.add(new PrefixValue("PN", pnautor[1]));
      }
    }
    for (int i = 0; i < field.getSubfieldCount(); i++) {
      Subfield subfield = field.getSubfield(i);
      if (subfield.getSecField() != null) {
        dest.add(new PrefixValue(field.getName() + subfield.getName(), subfield
            .getSecField().getName()));
        fieldToPrefixes(dest, subfield.getSecField());
      } else if (subfield.getSubsubfieldCount() > 0) {
        String content = prefixHandler.concatenateSubsubfields(subfield);
        if (!content.equals("")) {
          List<String> prefList = prefixMap.getPrefixes(field.getName()
              + subfield.getName());
          if (prefList != null) {
            Iterator<String> it = prefList.iterator();
            while (it.hasNext()) {
              String prefName = it.next();
              dest.add(new PrefixValue(prefName, content));
            }
          }
        }
      } else {
        List<String> prefList = prefixMap.getPrefixes(field.getName()
            + subfield.getName());
        if (prefList != null) {
          Iterator<String> it = prefList.iterator();
          while (it.hasNext()) {
            String prefName = it.next();
            if (prefName.equals("AU"))
              continue;
            if (!subfield.getContent().equals(""))
              dest.add(new PrefixValue(prefName, subfield.getContent()));
          }
        }
        dest.add(new PrefixValue(field.getName() + subfield.getName(), subfield
            .getContent()));
        // da bi u listu prefiksa dodali i potpolja npr 200a, posto je
        // sada samo TI u prefiksima
      }
    }
  }

  static PrefixConfig prefixConfig;

  static PrefixHandler prefixHandler;

  static PrefixMap prefixMap;
  private static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
  static {
    try {
      prefixConfig = PrefixConfigFactory.getPrefixConfig();
      prefixHandler = prefixConfig.getPrefixHandler();
      prefixMap = prefixConfig.getPrefixMap();
    } catch (Exception ex) {
      ex.printStackTrace();
    }
  }
}
