package amin.databatch.api;

import amin.databatch.entity.UploadedFile;
import amin.databatch.repository.UploadedFileRepository;
import amin.databatch.service.UploadedFileService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.mvc.method.annotation.MvcUriComponentsBuilder;

import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.stream.Collectors;

@RestController
@CrossOrigin("http://localhost:8081")
@RequestMapping(path = "api/v1/file")
@Slf4j
public class ApiController {

    @Autowired
    private FileStorageService storageService;

    @Autowired
    UploadedFileService uploadedFileService;

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    UploadedFileRepository uploadedFileRepository;

    @Autowired
    @Qualifier("ExcelFileProcessingJob")
    private Job ExcelFileProcessingJob;

    @PostMapping("/upload")
    @CrossOrigin
    @Transactional
    public ResponseEntity<ResponseMessage> submitFile(@RequestParam("file") MultipartFile file) {
        String message = "";

        Random random = new Random();
        int randomWithNextInt = random.nextInt();

        String name = randomWithNextInt +"-"+ file.getOriginalFilename();
        try {
            storageService.save(file, name);

            // add to db
            UploadedFile file1 = new UploadedFile();
            file1.setFilename(name);
            file1.setProcessed(false);
            uploadedFileService.add(file1);

            message = "Uploaded the file successfully: " + name;
            return ResponseEntity.status(HttpStatus.OK).body(new ResponseMessage(message));
        } catch (Exception e) {
            message = "Could not upload the file: " + file.getOriginalFilename() + "!";
            log.debug(e.toString());
            return ResponseEntity.status(HttpStatus.EXPECTATION_FAILED).body(new ResponseMessage(message));
        }
    }

    @PostMapping("/launch")
    @CrossOrigin
    public ResponseEntity<ResponseMessage> launch(@RequestParam("filename") String filename) throws JobInstanceAlreadyCompleteException, JobExecutionAlreadyRunningException, JobParametersInvalidException, JobRestartException {
        JobParameters jobParameters = new JobParametersBuilder().addString("filename", filename).toJobParameters();
        // launch job only if file exists in DB and is not processed
        Optional<UploadedFile> optionalUploadedFile = uploadedFileRepository.findByFilename(filename);
        if (optionalUploadedFile.isEmpty()) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(new ResponseMessage("File doesn't exist"));
        }
        if (optionalUploadedFile.get().isProcessed()) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(new ResponseMessage("File already processed"));
        }

        final JobExecution jobExecution = jobLauncher.run(ExcelFileProcessingJob, jobParameters);
        return ResponseEntity.status(HttpStatus.OK).body(new ResponseMessage("Success"));
    }

    @GetMapping("/files")
    public ResponseEntity<List<FileInfo>> getListFiles() {
        List<FileInfo> fileInfos = storageService.loadAll().map(path -> {
            String filename = path.getFileName().toString();
            String url = MvcUriComponentsBuilder
                    .fromMethodName(ApiController.class, "getFile", path.getFileName().toString()).build().toString();

            return new FileInfo(filename, url);
        }).collect(Collectors.toList());

        return ResponseEntity.status(HttpStatus.OK).body(fileInfos);
    }

    @GetMapping("/files/{filename:.+}")
    @ResponseBody
    public ResponseEntity<Resource> getFile(@PathVariable String filename) {
        Resource file = storageService.load(filename);
        return ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + file.getFilename() + "\"").body(file);
    }
}
