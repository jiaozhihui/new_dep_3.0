package com.bjvca.videocut

case class AdSeat4(
                   video_id: String,
                   project_id: String,
                   department_id: String,
                   media_name: String,
                   drama_name: String,
                   drama_type_name: String,
                   media_area_name: String,
                   class2_name: String,
                   class_type_id: String,
                   class3_name: String,
                   ad_seat_b_time: String,
                   ad_seat_e_time: String,
                   ad_seat_img: String,
                   resolution: String,
                   frame: Int,
                   tagTime: Long
                 )

/**
 *
 * video_id: String,            视频id
 * media_name: String,          视频名
 * drama_name: String,          视频类型名（电影电视剧）
 * drama_type_name: String,     视频风格名（武侠都市）
 * media_area_name: String,     地区名
 * class2_name: String,         二级标签名
 * class_type_id: String,       一级标签类型
 * class3_name: String,         三级标签名
 * ad_seat_b_time: String,      开始时间
 * ad_seat_e_time: String,      结束时间
 * ad_seat_img: String          封面图
 * resolution: String           分辨率
 * frame： Int                  帧数
 * tagTime: String              标签时长（用作分析标签时长占比）
 *
 */
