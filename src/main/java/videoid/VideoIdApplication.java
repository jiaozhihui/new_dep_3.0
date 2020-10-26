package videoid;

import com.bjvca.videocut.AllCleand;
import com.bjvca.videocut.TestDemo;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;
import videoid.controller.VidController;

@SpringBootApplication
@EnableAsync
public class VideoIdApplication {

    public static void main(String[] args) {
        TestDemo.init();

        SpringApplication.run(VideoIdApplication.class, args);
    }

}
