package amin.databatch.processor;

import amin.databatch.entity.FileDTO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemProcessor;

@Slf4j
public class FlatFileProcessor implements ItemProcessor<FileDTO, FileDTO> {
    @Override
    public FileDTO process(FileDTO fileDTO) {
        if (fileDTO != null) {
            fileDTO.setSum(fileDTO.getColumn_1()+fileDTO.getColumn_2());
        }

        log.debug("processing {}", fileDTO);
        return fileDTO;
    }
}
