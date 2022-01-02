use crate::utils::byte_buffer::*;
use bitstream_io::{BigEndian, BitRead, BitReader, BitWrite, BitWriter};
use std::mem::transmute;

pub fn compress(data: &[[u8; 8]], buf: &mut ByteBuffer) -> usize {
    // Code below inspire heavily from
    // https://github.com/jeromefroe/tsz-rs

    let mut buf: BitWriter<&mut ByteBuffer, BigEndian> = BitWriter::new(buf);
    let mut written = 0;

    // Store the length
    buf.write(64, data.len() as u64).unwrap();
    written += 64;

    // Store the first value
    let mut bits = unsafe { transmute::<f64, u64>(f64::from_be_bytes(data[0])) };
    buf.write(64, bits).unwrap();
    written += 64;

    // Keep track of last leading and trailing zeros
    let mut leading: u32 = 64;
    let mut trailing: u32 = 64;

    for v in data[1..].iter() {
        let cur_bits = unsafe { transmute::<f64, u64>(f64::from_be_bytes(*v)) };
        let xor = cur_bits ^ bits;

        if xor == 0 {
            // If xor is 0, values are the same, store a 0 bit
            buf.write_bit(false).unwrap();
            written += 1;
        } else {
            buf.write_bit(true).unwrap();
            written += 1;
            let cur_leading = xor.leading_zeros();
            let cur_trailing = xor.trailing_zeros();

            if cur_leading >= leading && cur_trailing >= trailing {
                // The start and end of the sig. bits in cur_xor is within the start and end of the
                // sig. bits in the previous xor. Store a zero bit and the sig. bits of the cur xor
                buf.write_bit(false).unwrap();
                written += 1;
                let sig_bits = 64 - leading - trailing;
                // actual value to write 000..110..000, number of sig_bits = 3
                // want to write 110
                // actual value >> trailing bits = 000...110
                // write (000..110, 3)
                let to_write = xor.wrapping_shr(trailing);
                buf.write(sig_bits, to_write).unwrap();
                written += sig_bits
            } else {
                buf.write_bit(true).unwrap();
                written += 1;

                // write the number of leading zeros (max 6 bits needed)
                buf.write(6, cur_leading).unwrap();
                written += 6;

                // write the number of sig. bits
                // if significant_digits is 64 we cannot encode it using 6 bits, however since
                // significant_digits is guaranteed to be at least 1 we can subtract 1 to ensure
                // significant_digits can always be expressed with 6 bits or less
                let sig_bits = 64 - cur_leading - cur_trailing;
                buf.write(6, sig_bits - 1).unwrap();
                written += 6;

                // write the actual sig. bits from the xor
                buf.write(sig_bits, xor.wrapping_shr(cur_trailing)).unwrap();
                written += sig_bits;

                // update leading and trailing metadata
                leading = cur_leading;
                trailing = cur_trailing;
            }

            // Finally, update bits to this value
            bits = cur_bits;
        }
    }
    buf.byte_align().unwrap();
    written as usize / 8 + (written % 8 > 0) as usize
}

/// Decompresses data into buf
/// Returns the number of bytes read from data and number of items decompressed.
/// Panics if buf is not long enough.
pub fn decompress(data: &[u8], buf: &mut Vec<[u8; 8]>) -> (usize, usize) {
    // Code below inspire heavily from
    // https://github.com/jeromefroe/tsz-rs

    let mut data: BitReader<&[u8], BigEndian> = BitReader::new(data);
    //let mut idx = 0;
    let mut read = 0;

    // Read the length
    let len = data.read::<u64>(64).unwrap() as usize;
    read += 64;

    // Read first value
    let mut bits = data.read::<u64>(64).unwrap();
    buf.push(unsafe { transmute::<u64, f64>(bits).to_be_bytes() });
    //idx += 1;
    read += 64;

    let mut leading = 0;
    let mut trailing = 0;

    while buf.len() < len {
        //println!("Decompress Leading: {} Trailing: {}", leading, trailing);
        let control = data.read_bit().unwrap();

        // Control bit = 0 means current is the same as the previous
        if !control {
            buf.push(buf[buf.len() - 1]);
        }
        // Otherwise, need to read the next bit to see if the sig. bits are w/in a window
        else {
            // If the next bit is set, update the leading and trailing bits information
            // 11 condition
            if data.read_bit().unwrap() {
                leading = data.read::<u32>(6).unwrap();
                let sig_bits = data.read::<u32>(6).unwrap() + 1;
                trailing = 64 - leading - sig_bits;
                read += 12;
            }

            // Both 11 and 10 conditions
            // Read the number of sig. bits
            let sig_bits = 64 - leading - trailing;
            let cur_bits = data.read::<u64>(sig_bits).unwrap();
            read += 64;

            // shift bits to proper position, then XOR with previous bits, assign to index
            // number of significant bits: 3
            // actual significant bits: 110 (in binary)
            // leading zeros: 18
            // trailing zeros: 64 - 18 - 3 = 43
            // 110 = data.read(sig_bits)
            // 110 = 00000....0000110 = 6
            // actual value: 0000...110...0000
            //               [ 18  ]   [  43 ]
            // 6 << 43 = actual value
            bits ^= cur_bits << trailing;
            buf.push(unsafe { transmute::<u64, f64>(bits) }.to_be_bytes());
        }
    }

    let bytes_read = read as usize / 8 + (read % 8 > 0) as usize;

    (bytes_read, len)
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test_utils::*;

    //#[test]
    //fn compress_decompress_values() {
    //    let data = UNIVARIATE_DATA.clone();
    //    let mut buf = vec![0u8; 4096];
    //    for (_id, samples) in data[..].iter().enumerate() {
    //        let raw = samples.1.iter().map(|x| x.values[0]).collect::<Vec<f64>>();
    //        for lo in (0..raw.len()).step_by(256) {
    //            let mut byte_buf = ByteBuffer::new(&mut buf[..]);
    //            let hi = raw.len().min(lo + 256);
    //            let exp = &raw[lo..hi];
    //            let mut res = vec![0.; hi - lo];
    //            let bytes = compress(exp, &mut byte_buf);
    //            let sz = byte_buf.len();
    //            decompress(&buf[..sz], &mut res[..]);
    //            assert_eq!(res, exp);
    //        }
    //    }
    //}

    //#[test]
    //fn compress_decompress_full() {
    //    let data = UNIVARIATE_DATA.clone();
    //    let mut buf = [0u8; 8192];
    //    for (_id, samples) in data[..].iter().enumerate() {

    //        // Re-organize data into columns
    //        let nvars = samples.1[0].values.len();
    //        let raw_time = samples.1.iter().map(|x| x.ts).collect::<Vec<Dt>>();
    //        let mut variables = Vec::new();
    //        for i in 0..nvars {
    //            let var = samples.1.iter().map(|x| x.values[i]).collect::<Vec<f64>>();
    //            variables.push(var);
    //        }

    //        for lo in (0..raw_time.len()).step_by(256) {

    //            let hi = raw_time.len().min(lo + 256);

    //            let mut segment = Segment::new();
    //            segment.timestamps.extend_from_slice(&raw_time[lo..hi]);
    //            for i in 0..nvars {
    //                segment.values.extend_from_slice(&variables[i][lo..hi]);
    //            }

    //            let bytes = xor_compression(&segment, &mut buf[..]);
    //            let mut result = Segment::new();
    //            let mut header = Header::default();
    //            header.len = hi - lo;
    //            xor_decompression(&header, &mut result, &buf[..bytes]);

    //            assert_eq!(result.timestamps, segment.timestamps);
    //            assert_eq!(result.len(), segment.len());
    //            for v in 0..nvars {
    //                assert_eq!(result.variable(v), segment.variable(v));
    //            }
    //        }
    //    }
    //}
}
