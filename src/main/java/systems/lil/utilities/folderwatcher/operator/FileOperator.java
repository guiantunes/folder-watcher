package systems.lil.utilities.folderwatcher.operator;

import io.reactivex.schedulers.Schedulers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import systems.lil.utilities.folderwatcher.watcher.FolderWatcher;

import java.io.File;
import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

@Service
public class FileOperator {

    private static final Logger logger = LoggerFactory.getLogger(FileOperator.class);

    private FolderWatcher folderWatcher;
    private SecureRandom secureRandom = SecureRandom.getInstanceStrong();
    private Path rootPath;

    public FileOperator(FolderWatcher folderWatcher, @Value("${folderWatcher.pathToDir}") String rootPath) throws NoSuchAlgorithmException {
        this.folderWatcher = folderWatcher;
        this.rootPath = Path.of(rootPath).toAbsolutePath();
        init();
    }

    private void init() {
        folderWatcher.getSubject()
                .parallel()
                .runOn(Schedulers.io())
                .map(this::processFile)
                .sequential().subscribe();
    }

    private Path processFile(Path filePath) throws InterruptedException {

        long time = secureRandom.nextInt(10) * 500;
        logger.info("Processing file: [{}] Estimated: [{}ms]", filePath, time);
        Thread.sleep(time);
        File file = filePath.toFile();
        if (file.exists()) {
            logger.info("File Exists Absolute Path [{}]", file.getAbsolutePath());
            file.delete();
            File parentDir = file.getParentFile();
            if (parentDir.isDirectory()) {
                while (!parentDir.toPath().equals(rootPath)) {
                    if (parentDir.list().length <= 0) {
                        parentDir.delete();
                        parentDir = parentDir.getParentFile();
                    }
                }
            }
        }
        logger.info("Processed file: [{}]", filePath);
        return filePath;
    }
}
