package videoid.controller;

import com.bjvca.videocut.AllCleand;
import com.bjvca.videocut.Cleaned1;
import com.bjvca.videocut.TestDemo;
import org.springframework.context.annotation.Scope;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;


@RestController
@Component
public class VidController {
    @RequestMapping("vid")
    @Async
    public void test1(@RequestParam("video_id") String video_id) {
//        Cleaned1.fun(video_id);
//        System.out.println(video_id);
        TestDemo.fun(TestDemo.getsession(), video_id);
    }

}
