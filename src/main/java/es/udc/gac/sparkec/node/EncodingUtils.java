/*
 * (C) Copyright 2017 The CloudEC Project and others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 * Contributors:
 *      Wei-Chun Chung (wcchung@iis.sinica.edu.tw)
 *      Chien-Chih Chen (rocky@iis.sinica.edu.tw)
 * 
 * CloudEC Project:
 *      https://github.com/CSCLabTW/CloudEC/
 */

package es.udc.gac.sparkec.node;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;

/**
 * This class is based off Utils from CloudEC. It contains many static utility methods
 * to help encoding, decoding, and operating over Sequences, Qualities and
 * nodes. Also, it holds the constants needed to operate with the original
 * CloudEC Nodes format.
 */
public class EncodingUtils implements Serializable {

	private static final long serialVersionUID = 1L;

	
	// CloudEC node encoding messages 
	
	/**
	 * Node global message code
	 */
	public static final String MSGNODE = "N";

	/**
	 * Sequence field code
	 */
	public static final String SEQ = "s";
	
	/**
	 * Quality field code
	 */
	public static final String QV = "q";
	
	/**
	 * Coverage field code
	 */
	public static final String COVERAGE = "v";
	
	/**
	 * Unique field code
	 */
	public static final String UNIQUE = "u";
	
	/**
	 * IGNP field code
	 */
	public static final String IGNP = "p";
	
	/**
	 * IGNF field code
	 */
	public static final String IGNF = "f";

	
	
	// Quality encoding values
	
	/**
	 * Lower value for QV
	 */
	private static final short QV_LOW = 0;
	
	/**
	 * Upper value for QV
	 */
	private static final short QV_UP = 40;
	
	/**
	 * Base value for QV
	 */
	private static final short QV_BASE = 33;
	
	/**
	 * Good value for QV
	 */
	public static final short QV_GOOD = 20;
	
	/**
	 * Default value for fixed bases of QV
	 */
	public static final short QV_FIX = QV_BASE;

	/**
	 * Deflating/Inflating factor for QV
	 */
	private static final short QV_FLATE_FACTOR = 3;
	
	/**
	 * Upper bound of deflated QV
	 */
	private static final short QV_FLATE_UP = QV_UP / QV_FLATE_FACTOR;

	/**
	 * Radius for QV smooth algorithm
	 */
	private static final short QV_SMOOTH_RADIOUS = 2;



	// DNA Encoding values
	
	/**
	 * DNA Bases
	 */
	private static final String[] dnachars = { "A", "T", "C", "G", "N" };
	
	/**
	 * DNA Bases, including invalid values
	 */
	private static final String[] codechars = { "A", "T", "C", "G", "N", "X" };

	/**
	 * Map to encode DNA bases Strings
	 */
	private static final HashMap<String, String> str2dna_ = initializeSTR2DNA();
	
	/**
	 * Map to decode DNA bases Strings
	 */
	private static final HashMap<String, String> dna2str_ = initializeDNA2STR();

	/**
	 * Map to encode DNA bases Strings, including invalid values
	 */
	private static final HashMap<String, String> str2code_ = initializeSTR2CODE();
	
	/**
	 * Map to decode DNA bases Strings, including invalid values
	 */
	private static final HashMap<String, String> code2str_ = initializeCODE2STR();
	
	
	// Initialization of encoding maps

	
	
	/**
	 * Initialize DNA encoding map.
	 * Examples of encoding would be: A=>A, AA=>B, AT=>C, AC=>D, AG=>E, ...
	 * @return The encoding map
	 */
	private static HashMap<String, String> initializeSTR2DNA() {
		int num = 0;
		int asciibase = 'A';

		HashMap<String, String> retval = new HashMap<>();

		for (int xi = 0; xi < dnachars.length; xi++) {
			retval.put(dnachars[xi], Character.toString((char) (num + asciibase)));

			num++;

			for (int yi = 0; yi < dnachars.length; yi++) {
				retval.put(dnachars[xi] + dnachars[yi], Character.toString((char) (num + asciibase)));
				num++;
			}
		}

		for (int xi = 0; xi < dnachars.length; xi++) {
			for (int yi = 0; yi < dnachars.length; yi++) {
				String m = retval.get(dnachars[xi] + dnachars[yi]);

				for (int zi = 0; zi < dnachars.length; zi++) {
					retval.put(dnachars[xi] + dnachars[yi] + dnachars[zi], m + retval.get(dnachars[zi]));

					for (int wi = 0; wi < dnachars.length; wi++) {
						retval.put(dnachars[xi] + dnachars[yi] + dnachars[zi] + dnachars[wi],
								m + retval.get(dnachars[zi] + dnachars[wi]));

						for (int ui = 0; ui < dnachars.length; ui++) {
							retval.put(dnachars[xi] + dnachars[yi] + dnachars[zi] + dnachars[wi] + dnachars[ui],
									m + retval.get(dnachars[zi] + dnachars[wi]) + retval.get(dnachars[ui]));
						}
					}
				}
			}
		}

		return retval;
	}


	/**
	 * Initialize DNA decoding map.
	 * Examples of decoding would be: A=>A, B=>AA, C=>AT, D=>AC, E=>AG, ...
	 * @return The decoding map
	 */
	private static HashMap<String, String> initializeDNA2STR() {
		int num = 0;
		int asciibase = 65;

		HashMap<String, String> retval = new HashMap<>();

		for (int xi = 0; xi < dnachars.length; xi++) {
			retval.put(Character.toString((char) (num + asciibase)), dnachars[xi]);

			num++;

			for (int yi = 0; yi < dnachars.length; yi++) {
				retval.put(Character.toString((char) (num + asciibase)), dnachars[xi] + dnachars[yi]);
				num++;
			}
		}

		return retval;
	}

	/**
	 * Initialize DNA encoding map, including invalid bases.
	 * Examples of decoding would be: A=>A, B=>AA, C=>AT, D=>AC, E=>AG, ...
	 * @return The decoding map
	 */
	private static HashMap<String, String> initializeSTR2CODE() {
		int num = 0;
		int asciibase = 'A';

		HashMap<String, String> retval = new HashMap<>();

		for (int xi = 0; xi < codechars.length; xi++) {
			retval.put(codechars[xi], Character.toString((char) (num + asciibase)));

			num++;

			for (int yi = 0; yi < codechars.length; yi++) {
				retval.put(codechars[xi] + codechars[yi], Character.toString((char) (num + asciibase)));
				num++;
			}
		}

		for (int xi = 0; xi < codechars.length; xi++) {
			for (int yi = 0; yi < codechars.length; yi++) {
				String m = retval.get(codechars[xi] + codechars[yi]);

				for (int zi = 0; zi < codechars.length; zi++) {
					retval.put(codechars[xi] + codechars[yi] + codechars[zi], m + retval.get(codechars[zi]));

					for (int wi = 0; wi < codechars.length; wi++) {
						retval.put(codechars[xi] + codechars[yi] + codechars[zi] + codechars[wi],
								m + retval.get(codechars[zi] + codechars[wi]));
						for (int ui = 0; ui < codechars.length; ui++) {
							retval.put(codechars[xi] + codechars[yi] + codechars[zi] + codechars[wi] + codechars[ui],
									m + retval.get(codechars[zi] + codechars[wi]) + retval.get(codechars[ui]));
							for (int vi = 0; vi < codechars.length; vi++) {
								retval.put(
										codechars[xi] + codechars[yi] + codechars[zi] + codechars[wi] + codechars[ui]
												+ codechars[vi],
										m + retval.get(codechars[zi] + codechars[wi])
												+ retval.get(codechars[ui] + codechars[vi]));
							}
						}
					}
				}
			}
		}

		return retval;
	}

	/**
	 * Initialize DNA decoding map, including invalid bases.
	 * Examples of decoding would be: A=>A, B=>AA, C=>AT, D=>AC, E=>AG, ...
	 * @return The decoding map
	 */
	private static HashMap<String, String> initializeCODE2STR() {
		int num = 0;
		int asciibase = 65;

		HashMap<String, String> retval = new HashMap<>();

		for (int xi = 0; xi < codechars.length; xi++) {
			retval.put(Character.toString((char) (num + asciibase)), codechars[xi]);

			num++;

			for (int yi = 0; yi < codechars.length; yi++) {
				retval.put(Character.toString((char) (num + asciibase)), codechars[xi] + codechars[yi]);
				num++;
			}
		}

		return retval;
	}

	// 

	/**
	 * Encode DNA sequence
	 * @param seq The original DNA sequence
	 * @return The encoded DNA sequence
	 */
	private static String dnaEncode(final String seq) {
		StringBuilder sb = new StringBuilder();

		int l = seq.length();

		int offset = 0;

		while (offset < l) {
			int r = l - offset;

			if (r >= dnachars.length) {
				sb.append(str2dna_.get(seq.substring(offset, offset + dnachars.length)));
				offset += dnachars.length;
			} else {
				sb.append(str2dna_.get(seq.substring(offset, offset + r)));
				offset += r;
			}
		}

		return sb.toString();
	}

	/**
	 * Decode DNA sequence
	 * @param seq The encoded DNA sequence
	 * @return The original DNA sequence
	 */
	private static String dnaDecode(final String encodedSEQ) {
		StringBuilder sb = new StringBuilder();

		for (int i = 0; i < encodedSEQ.length(); i++) {
			sb.append(dna2str_.get(encodedSEQ.substring(i, i + 1)));
		}

		return sb.toString();
	}

	/**
	 * Encode DNA sequence, including invalid bases
	 * @param seq The original DNA sequence
	 * @return The encoded DNA sequence
	 */
	private static String codeEncode(final String codes) {
		StringBuilder sb = new StringBuilder();

		int l = codes.length();

		int offset = 0;

		while (offset < l) {
			int r = l - offset;

			if (r >= codechars.length) {
				sb.append(str2code_.get(codes.substring(offset, offset + codechars.length)));
				offset += codechars.length;
			} else {
				sb.append(str2code_.get(codes.substring(offset, offset + r)));
				offset += r;
			}
		}

		return sb.toString();
	}

	
	/**
	 * Decode DNA sequence, including invalid bases
	 * @param seq The encoded DNA sequence
	 * @return The original DNA sequence
	 */
	private static String codeDecode(final String encodedCODES) {
		StringBuilder sb = new StringBuilder();

		for (int i = 0; i < encodedCODES.length(); i++) {
			sb.append(code2str_.get(encodedCODES.substring(i, i + 1)));
		}

		return sb.toString();
	}

	/**
	 * Encode DNA sequence
	 * @param seq The original DNA sequence
	 * @return The encoded DNA sequence
	 */
	public static byte[] seqEncode(final byte[] seq) {
		return dnaEncode(new String(seq)).getBytes();
	}

	/**
	 * Decode DNA sequence
	 * @param encodedSEQ The encoded DNA sequence
	 * @return The original DNA sequence
	 */
	public static byte[] seqDecode(final byte[] encodedSEQ) {
		return dnaDecode(new String(encodedSEQ)).getBytes();
	}

	/**
	 * Encode sequence correction data
	 * @param corrMsg The raw correction data
	 * @return The encoded correction data
	 */
	public static String corrEncode(final String corrMsg) {
		return codeEncode(corrMsg);
	}

	/**
	 * Decode sequence correction data
	 * @param encodedMsg The encoded correction data
	 * @return The raw correction data
	 */
	public static String corrDecode(final String encodedMsg) {
		return codeDecode(encodedMsg);
	}

	// Quality encoding

	/**
	 * Applies input operations the first time the quality values are read.
	 * @param qv The quality values
	 * @param reuseBuffer Whether a new byte array should be allocated
	 * @return The quality values, preprocessed
	 */
	public static byte[] qvInputConvert(final byte[] qv, boolean reuseBuffer) {
		byte[] buffer = null;
		if (reuseBuffer) {
			buffer = qv;
		} else {
			buffer = new byte[qv.length];
		}
		for (int i = 0; i < qv.length; i++) {
			if (qv[i] - QV_BASE < QV_LOW) {
				buffer[i] = (byte) (QV_BASE + QV_LOW);
			} else if (qv[i] - QV_BASE > QV_UP) {
				buffer[i] = (byte) (QV_BASE + QV_UP);
			} else {
				buffer[i] = qv[i];
			}
		}
		return buffer;
	}

	/**
	 * Convert ASCII based QVs into values (as byte array).
	 * @param asciiQV The ASCII based QVs
	 * @param reverse If the QV has to be reversed
	 * @return The new encoded QV
	 */
	public static byte[] qvValueConvert(final byte[] asciiQV, final boolean reverse) {
		byte[] qv_int = new byte[asciiQV.length];

		for (int i = 0; i < qv_int.length; i++) {
			int target = i;

			if (reverse) {
				target = qv_int.length - 1 - i;
			}

			qv_int[i] = (byte) (asciiQV[target] - QV_BASE);
		}

		return qv_int;
	}

	/**
	 * Convert internal QV encoding to output format.
	 * @param qv The QV, with the internal encoding
	 * @return The QV, with the output format
	 */
	public static String qvOutputConvert(final String qv) {
		StringBuilder sb = new StringBuilder(qv);
		
		for (int i = 0; i < sb.length(); i++) {

			sb.setCharAt(i, (char) ((Math.min((sb.charAt(i) - QV_BASE) / QV_FLATE_FACTOR, QV_FLATE_UP) * QV_FLATE_FACTOR + (QV_FLATE_FACTOR / 2)) + QV_BASE));
		}
		
		return sb.toString();
	}

	/**
	 * Compress a byte array containing bits, into a byte array containing hex values
	 * @param data The byte array containing bits 
	 * @return The byte array containing hex values
	 */
	public static byte[] str2hex(final byte[] data) {

		byte[] buffer = new byte[data.length / 4 + (((data.length % 4) == 0) ? 0 : 1)];
		int index = 0;
		byte b = 0x00;
		for (int i = 0; i < data.length; i++) {
			b <<= 1;

			b |= data[i];

			if ((i + 1) % 4 == 0) {
				buffer[index++] = (byte) Character.toUpperCase(String.format("%x", b).charAt(0));
				b = 0x00;
			}
		}

		return buffer;
	}

	/**
	 * Gets the DNA base, given an integer representation of it
	 * @param base The integer representation of the base
	 * @return The base
	 */
	public static char idx2char(final int base) {
		return dnachars[base].charAt(0);
	}


	/**
	 * Gets an integer representation of a DNA base
	 * @param base The DNA base
	 * @return The integer representation
	 */
	public static int char2idx(final char base) {
		for (int i = 0; i < dnachars.length; i++) {
			if (base == dnachars[i].charAt(0)) {
				return i;
			}
		}

		return (dnachars.length - 1);
	}

	/**
	 * Deflate 8-bit continuous QV to 4-bit continuous value 
	 * @param qv the 8-bit continuous QV
	 * @return The 4-bit continuous value 
	 */
	public static byte[] qvDeflate(final byte[] qv) {
		byte[] buffer = Arrays.copyOf(qv, qv.length);

		for (int i = 0; i < buffer.length; i++) {
			int val = buffer[i] - QV_BASE;

			buffer[i] = (byte) (Math.min(val / QV_FLATE_FACTOR, QV_FLATE_UP) + QV_BASE);
		}

		return buffer;
	}

	/**
	 * Inflate 8-bit continuous QV to 4-bit continuous value 
	 * @param qv the 4-bit continuous QV
	 * @return The 8-bit continuous value 
	 */
	public static byte[] qvInflate(final byte[] qv) {
		byte[] buffer = new byte[qv.length];

		for (int i = 0; i < buffer.length; i++) {
			int val = (byte) (qv[i] - QV_BASE);
			buffer[i] = (byte) ((val * QV_FLATE_FACTOR + (QV_FLATE_FACTOR / 2)) + QV_BASE);
		}
		return buffer;
	}

	/**
	 * Applies an smooth algorithm to the QV
	 * @param qv The original QV
	 * @return The smooth QV
	 */
	public static byte[] qvSmooth(final byte[] qv) {
		byte[] buffer = Arrays.copyOf(qv, qv.length);
		byte QV_BASE = (byte) EncodingUtils.QV_BASE;
		for (int i = 0; i < buffer.length; i++) {
			int start = Math.max(i - QV_SMOOTH_RADIOUS, 0);
			int end = Math.min(i + QV_SMOOTH_RADIOUS, qv.length - 1);

			byte min = (byte) (qv[i] - QV_BASE);
			for (int j = start; j <= end; j++) {
				min = (byte) Math.min(min, qv[j] - QV_BASE);
			}
			buffer[i] = (byte) (min + QV_BASE);
		}
		return buffer;
	}

	/**
	 * Decodes into a bit String an hex encoded IGN String.
	 * @param encoded The hex encoded IGN String
	 * @return The bit encoded IGN String
	 */
	public static String decodeIGN(final String encoded) {
		
		StringBuilder sb = new StringBuilder();

		for (int i = 0; i < encoded.length(); i++) {
			int c = encoded.charAt(i) - '0';
			if (c > 10) {
				c = c - 7;
			}
			
			sb.append(((c & ((byte)0b1000)) > 0) ? "1" : "0");
			sb.append(((c & ((byte)0b0100)) > 0) ? "1" : "0");
			sb.append(((c & ((byte)0b0010)) > 0) ? "1" : "0");
			sb.append(((c & ((byte)0b0001)) > 0) ? "1" : "0");

		}
		return sb.toString();
	}

	private EncodingUtils() {
	}

}
