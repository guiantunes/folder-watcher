package systems.lil.utilities.folderwatcher.watcher;

import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.Observable;
import io.reactivex.subjects.PublishSubject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

@Service
public class FolderWatcher {

    private static final Logger logger = LoggerFactory.getLogger(FolderWatcher.class);

    private WatchService watchService;
    private String pathToDir;
    private PublishSubject<Path> subject;


    public FolderWatcher(@Value("${folderWatcher.pathToDir}") String pathToDir) throws IOException {
        this.pathToDir = pathToDir;
        subject = PublishSubject.create();
    }

    @EventListener (ContextRefreshedEvent.class)
    public void init() throws IOException {
        watchService = FileSystems.getDefault().newWatchService();
        Path path = Paths.get(pathToDir).toAbsolutePath();
        path.register(watchService, StandardWatchEventKinds.ENTRY_CREATE);

        publishPath(path, null);

        Observable.interval(0, 1, TimeUnit.SECONDS)
                .subscribe(aLong -> {
                    WatchKey watchKey = watchService.poll();
                    if (watchKey != null) {
                        for (WatchEvent<?> event : watchKey.pollEvents()) {
                            logger.info("File Identified: [{}]", event.context().toString());
                            publishPath(path, (Path) event.context());
                        }
                        watchKey.reset();
                    }
                });
    }

    private void publishPath(Path context, Path file) throws IOException {
        if (context != null) {
            if (file != null) {
                if (Files.isRegularFile(context.resolve(file))) {
                    publishRegularFile(context, file);
                } else if (Files.isDirectory(context.resolve(file))) {
                    publishDirectory(context.resolve(file));
                }
            } else {
                publishDirectory(context);
            }
        }
    }

    private void publishDirectory(Path path) throws IOException {
        try (Stream<Path> walk = Files.walk(path)) {
            walk.forEach(currentPath -> {
                if (!currentPath.equals(path)) {
                    logger.info("Current File [{}] regular: [{}]", currentPath.toAbsolutePath(), Files.isRegularFile(currentPath));
                    if (Files.isRegularFile(currentPath)) {
                        publishRegularFile(path, currentPath);
                    } else if (Files.isDirectory(currentPath)) {
                        try {
                            publishDirectory(path.resolve(currentPath));
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
            });
        }
    }

    private void publishRegularFile(Path parentPath, Path filePath) {
        subject.onNext(parentPath.resolve(filePath));
    }

    public Flowable<Path> getSubject() {
        return subject.toFlowable(BackpressureStrategy.BUFFER);
    }

}
