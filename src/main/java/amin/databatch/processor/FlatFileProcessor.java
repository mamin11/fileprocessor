package amin.databatch.processor;

import amin.databatch.entity.FileDTO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemProcessor;

@Slf4j
public class FlatFileProcessor implements ItemProcessor<FileDTO, FileDTO> {
    @Override
    public FileDTO process(FileDTO fileDTO) throws Exception {
        log.debug("processing {}", fileDTO);
        return null;
    }
}
