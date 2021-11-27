package amin.databatch.service;

import amin.databatch.entity.UploadedFile;
import amin.databatch.repository.UploadedFileRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class UploadedFileService {
    @Autowired
    UploadedFileRepository repository;

    public List<UploadedFile> findAll() {
        return repository.findAll();
    }

    public void add(UploadedFile file) {
        // validate
        if (file.getFilename().length() <= 0 || file.getFilename() == null) {
            throw new IllegalStateException("File validation failed");
        }
        repository.save(file);
    }

    public void updateFile(Long id, boolean processed) {
        UploadedFile file = repository.findById(id).orElseThrow(() -> new IllegalStateException("File Not in database"));
        file.setProcessed(processed);
    }
}
