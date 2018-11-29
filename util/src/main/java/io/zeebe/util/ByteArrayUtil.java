/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.zeebe.util;

public final class ByteArrayUtil {

  private ByteArrayUtil() {}

  public static byte[] concat(byte[]... buffers) {
    int length = 0;
    for (byte[] buffer : buffers) {
      length += buffer.length;
    }

    return concatTo(new byte[length], buffers);
  }

  public static byte[] concatTo(byte[] dest, byte[]... buffers) {
    int offset = 0;
    for (byte[] buffer : buffers) {
      System.arraycopy(buffer, 0, dest, offset, buffer.length);
      offset += buffer.length;
    }

    return dest;
  }
}
