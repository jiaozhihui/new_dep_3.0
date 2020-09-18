package videoid.controller;

import com.bjvca.videocut.Cleaned1;
import org.springframework.context.annotation.Scope;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;


@RestController
@Component
public class VidController {
    @RequestMapping("vid")
    @Async
    public String test1(int video_id) throws Exception {
//        Cleaned1.fun(video_id);
        System.out.println(video_id);
        return "video_id：" + video_id + "，片段已生产完毕";
    }

}
