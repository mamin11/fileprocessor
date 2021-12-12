package amin.databatch.mapper;

import amin.databatch.entity.UploadedFile;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

public class UploadedFileMapper implements RowMapper<UploadedFile> {
    @Override
    public UploadedFile mapRow(ResultSet rs, int rowNum) throws SQLException {
        return UploadedFile
                .builder()
                .id(rs.getLong("id"))
                .filename(rs.getString("filename"))
                .processed(rs.getBoolean("processed"))
                .build();
    }
}
