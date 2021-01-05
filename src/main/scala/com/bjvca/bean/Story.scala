package com.bjvca.bean

case class Story1(
                   platform_id: String,
                   project_id: String,
                   media_id: String,
                   story_start: Int,
                   story_end: Int,
                   class_id: List[Int],
                   image: String
                 )

case class Story2(
                   platform_id: String,
                   project_id: String,
                   media_id: String,
                   story_start: Int,
                   story_end: Int,
                   timeLong: Int,
                   image: String
                 )