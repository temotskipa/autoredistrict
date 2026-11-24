package com.temotskipa.autoredistrict.data;

import tools.jackson.databind.SerializationFeature;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * Shared JSON mapper and file hashing.
 */
public final class Json {
    
    private static final LazyConstant<JsonMapper> MAPPER = LazyConstant.of(() -> JsonMapper.builder().enable(SerializationFeature.INDENT_OUTPUT).build());
    
    private Json() {
    }
    
    public static JsonMapper mapper() {
        return MAPPER.get();
    }
    
    /**
     * SHA-256 of a file as lower-case hex.
     */
    public static String sha256(Path file) {
        try (InputStream in = Files.newInputStream(file)) {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] buf = new byte[1 << 16];
            int r;
            while ((r = in.read(buf)) > 0) {
                digest.update(buf, 0, r);
            }
            return HexFormat.of().formatHex(digest.digest());
        } catch (IOException e) {
            throw new UncheckedIOException("Could not read " + file + " to hash it", e);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
