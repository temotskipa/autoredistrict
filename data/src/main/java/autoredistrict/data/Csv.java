package autoredistrict.data;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/** Minimal streaming CSV support (RFC 4180 quoting), enough for RDH files and our cache. */
public final class Csv {

    private Csv() {
    }

    /** Splits one CSV record. Quoted fields may contain commas and doubled quotes. */
    public static List<String> split(String line) {
        List<String> out = new ArrayList<>();
        StringBuilder field = new StringBuilder();
        boolean quoted = false;
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (quoted) {
                if (c == '"') {
                    if (i + 1 < line.length() && line.charAt(i + 1) == '"') {
                        field.append('"');
                        i++;
                    } else {
                        quoted = false;
                    }
                } else {
                    field.append(c);
                }
            } else if (c == '"') {
                quoted = true;
            } else if (c == ',') {
                out.add(field.toString());
                field.setLength(0);
            } else {
                field.append(c);
            }
        }
        out.add(field.toString());
        return out;
    }

    /**
     * Extracts the given columns from a record without building the whole field list. Returns
     * false when the record has fewer columns than needed. {@code wanted} must be sorted ascending.
     */
    public static boolean pick(String line, int[] wanted, String[] out) {
        int col = 0;
        int w = 0;
        int i = 0;
        int len = line.length();
        while (w < wanted.length && i <= len) {
            int start = i;
            String value;
            if (i < len && line.charAt(i) == '"') {
                StringBuilder sb = new StringBuilder();
                i++;
                while (i < len) {
                    char c = line.charAt(i);
                    if (c == '"') {
                        if (i + 1 < len && line.charAt(i + 1) == '"') {
                            sb.append('"');
                            i += 2;
                            continue;
                        }
                        i++;
                        break;
                    }
                    sb.append(c);
                    i++;
                }
                while (i < len && line.charAt(i) != ',') {
                    i++;
                }
                value = sb.toString();
            } else {
                while (i < len && line.charAt(i) != ',') {
                    i++;
                }
                value = null;
            }
            if (col == wanted[w]) {
                out[w++] = value != null ? value : line.substring(start, i);
            }
            col++;
            i++; // skip comma
        }
        return w == wanted.length;
    }

    /** Index of the first header matching one of the names, case-insensitively; -1 if none. */
    public static int indexOf(List<String> header, String... names) {
        for (String name : names) {
            for (int i = 0; i < header.size(); i++) {
                if (header.get(i).strip().equalsIgnoreCase(name)) {
                    return i;
                }
            }
        }
        return -1;
    }

    /** Reads the header line, stripping a UTF-8 BOM. */
    public static List<String> header(BufferedReader reader, String source) throws IOException {
        String line = reader.readLine();
        if (line == null) {
            throw new DataException(source + " is empty.");
        }
        if (!line.isEmpty() && line.charAt(0) == '﻿') {
            line = line.substring(1);
        }
        return split(line);
    }

    /** Parses a number field; blank means 0. */
    public static double number(String field, String column, String source) {
        String s = field == null ? "" : field.strip();
        if (s.isEmpty()) {
            return 0;
        }
        try {
            return Double.parseDouble(s);
        } catch (NumberFormatException e) {
            throw new DataException(source + " has a non-numeric value '" + s + "' in " + column + ".");
        }
    }

    /** Quotes a value for writing if it needs it. */
    public static String quote(String value) {
        if (value == null) {
            return "";
        }
        if (value.indexOf(',') < 0 && value.indexOf('"') < 0 && value.indexOf('\n') < 0) {
            return value;
        }
        return '"' + value.replace("\"", "\"\"") + '"';
    }

    /** Formats a double compactly and without locale surprises. */
    public static String num(double v) {
        if (v == Math.rint(v) && Math.abs(v) < 1e15) {
            return Long.toString((long) v);
        }
        return String.format(Locale.ROOT, "%.6f", v);
    }
}
