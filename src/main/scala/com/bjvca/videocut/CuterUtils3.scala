package com.bjvca.videocut

import com.alibaba.fastjson.{JSONArray, JSONObject}

import scala.collection.mutable.ListBuffer

/**
 * 统一的一套数据结构
 */
object CuterUtils3 {

  def seatToJSON(seats: ListBuffer[AdSeat]): JSONObject = {

    val temp = new JSONObject()

    val class3List = new JSONArray()
    val manList = new JSONArray()
    val objectList = new JSONArray()
    val actionList = new JSONArray()
    val senceList = new JSONArray()

    val class2List = new JSONArray()
//    val man2List = new JSONArray()
//    val object2List = new JSONArray()
//    val action2List = new JSONArray()
//    val sence2List = new JSONArray()

    val classImgList = new JSONArray()
//    val manImgList = new JSONArray()
//    val objectImgList = new JSONArray()
//    val actionImgList = new JSONArray()
//    val senceImgList = new JSONArray()

    var minBTime: String = seats.head.ad_seat_b_time
    var maxETime: String = seats.head.ad_seat_e_time

    for (seat <- seats) {

      if (seat.ad_seat_b_time.toLong < minBTime.toLong) minBTime = seat.ad_seat_b_time
      if (seat.ad_seat_e_time.toLong > maxETime.toLong) maxETime = seat.ad_seat_e_time

      val file1 = seat.video_id
      val file2 = seat.media_name
      val file3 = seat.drama_name
      val file4 = seat.drama_type_name
      val file5 = seat.media_area_name
      //      val file6 = seat.media_release_date
      val file7 = seat.class_type_id
      val file8 = seat.class3_name
      val file9 = seat.class2_name
      val file10 = seat.ad_seat_img

      temp.put("string_vid", file1)
      temp.put("media_name", file2)
      temp.put("string_drama_name", file3)
      temp.put("string_drama_type_name", file4)
      temp.put("string_media_area_name", file5)
      //      temp.put("string_media_release_date", file6)

      class3List.add(file8)
      class2List.add(file9)
      classImgList.add(file10)

      file7 match {
        case "4" =>
          manList.add(file8)
          //          man2List.add(file9)
          //          manImgList.add(file10)
        case "1" =>
          objectList.add(file8)
          //          object2List.add(file9)
          //          objectImgList.add(file10)
        case "3" =>
          actionList.add(file8)
          //          action2List.add(file9)
          //          actionImgList.add(file10)
        case "2" =>
          senceList.add(file8)
          //          sence2List.add(file9)
          //          senceImgList.add(file10)
      }

    }

    temp.put("string_class3_list", class3List)
    temp.put("string_man_list", manList)
    temp.put("string_object_list", objectList)
    temp.put("string_action_list", actionList)
    temp.put("string_sence_list", senceList)

    temp.put("string_class2_list", class2List)
//    temp.put("string_man2_list", man2List)
//    temp.put("string_object2_list", object2List)
//    temp.put("string_action2_list", action2List)
//    temp.put("string_sence2_list", sence2List)

    temp.put("string_class_img_list", classImgList)
//    temp.put("string_man_img_list", manImgList)
//    temp.put("string_object_img_list", objectImgList)
//    temp.put("string_action_img_list", actionImgList)
//    temp.put("string_sence_img_list", senceImgList)

    temp.put("string_time", minBTime + "_" + maxETime)
    temp.put("string_time_long", (maxETime.toLong - minBTime.toLong).toString)

    temp.put("resourceId", "1")

    temp
  }


}
