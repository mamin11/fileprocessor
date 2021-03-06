package amin.databatch.repository;

import amin.databatch.entity.UploadedFile;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface UploadedFileRepository extends JpaRepository<UploadedFile, Long> {
    Optional<UploadedFile> findByFilename(String filename);
}
