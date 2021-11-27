package amin.databatch.entity;

import lombok.Data;

import javax.persistence.*;

@Data
@Table
@Entity
public class UploadedFile {

    @Id
    @SequenceGenerator(
            name = "file_sequence",
            sequenceName = "file_sequence",
            allocationSize = 1
    )

    @GeneratedValue(
            strategy = GenerationType.SEQUENCE,
            generator = "file_sequence"
    )

    private Long id;
    private String filename;
    private boolean processed;
}
