package scalackh.client.utils;

import java.io.UnsupportedEncodingException;

// gist from jen20
public final class HexDump {
  public static String format(byte[] arr, int off, int len) {
    final int width = 16;

    byte[] array = new byte[len - off];
    System.arraycopy(arr, off, array, 0, len);

    StringBuilder builder = new StringBuilder();

    for (int rowOffset = off; rowOffset < off + len; rowOffset += width) {
      builder.append(String.format("%06d:  ", rowOffset));

      for (int index = 0; index < width; index++) {
        if (rowOffset + index < len) {
          builder.append(String.format("%02x ", array[rowOffset + index]));
        }
        else {
          builder.append("   ");
        }
      }

      if (rowOffset < array.length) {
        int asciiWidth = Math.min(width, array.length - rowOffset);
        builder.append("  |  ");
        try {
          byte[] safeArray = new byte[asciiWidth];
          System.arraycopy(array, rowOffset, safeArray, 0, asciiWidth);
          
          for(int i = 0; i < safeArray.length; i++) {
            byte b = safeArray[i];
            if(b < 0x20 || b > 0x7e) safeArray[i] = 0x2e;
          }

          builder.append(new String(safeArray, 0, asciiWidth, "UTF-8"));
        }
        catch (UnsupportedEncodingException ignored) {
          // if UTF-8 isn't available as an encoding then what can we do?!
        }
      }

      builder.append(String.format("%n"));
    }

    return builder.toString();
  }
}