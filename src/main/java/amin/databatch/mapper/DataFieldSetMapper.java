package amin.databatch.mapper;

import amin.databatch.entity.FileDTO;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.validation.BindException;

public class DataFieldSetMapper implements org.springframework.batch.item.file.mapping.FieldSetMapper<amin.databatch.entity.FileDTO> {
    @Override
    public FileDTO mapFieldSet(FieldSet fieldSet) throws BindException {
        FileDTO fileDTO = new FileDTO();
        fileDTO.setColumn_1(fieldSet.readInt("column_1"));
        fileDTO.setColumn_2(fieldSet.readInt("column_2"));
        return fileDTO;
    }
}
