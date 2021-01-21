package com.bjvca.videocut

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSONArray, JSONObject}

import scala.collection.mutable.ListBuffer

/**
 * 统一的一套数据结构
 */
object CuterUtils8 {

  def seatToJSON(seats: ListBuffer[AdSeat4]): JSONObject = {

    val temp = new JSONObject()

    val class3List = new JSONArray()
    val allTagTime = new JSONObject()
    val class3Time = new JSONArray()
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

    var minBTime: Long = seats.head.ad_seat_b_time.toLong
    var maxETime: Long = seats.head.ad_seat_e_time.toLong

    for (seat <- seats) {

      if (seat.ad_seat_b_time.toLong < minBTime) minBTime = seat.ad_seat_b_time.toLong
      if (seat.ad_seat_e_time.toLong > maxETime) maxETime = seat.ad_seat_e_time.toLong

      val video_id = seat.video_id
      val media_name = seat.media_name
      val drama_name = seat.drama_name
      val drama_type_name = seat.drama_type_name
      val media_area_name = seat.media_area_name
//      val media_release_date = seat.media_release_date
      val class_type_id = seat.class_type_id
      val class3_name = seat.class3_name
      val class2_name = seat.class2_name
      val ad_seat_img = seat.ad_seat_img
      val department_id = seat.department_id
      val project_id = seat.project_id
      val resolution = seat.resolution
      val frame = seat.frame
      val tagTime = seat.tagTime
      val ad_seat_b_time = seat.ad_seat_b_time
      val ad_seat_e_time = seat.ad_seat_e_time

      temp.put("string_vid", video_id)
      temp.put("media_name", media_name)
      temp.put("string_drama_name", drama_name)
      temp.put("string_drama_type_name", drama_type_name)
      temp.put("string_media_area_name", media_area_name)
//      temp.put("string_media_release_date", media_release_date)
      temp.put("department_id", department_id)
      temp.put("project_id", project_id)
      temp.put("resolution", resolution)
      temp.put("frame", frame)

      if (!class3List.contains(class3_name)){
        class3List.add(class3_name)
        allTagTime.put(class3_name,tagTime.toString)
      } else {
        allTagTime.replace(class3_name,(allTagTime.getLong(class3_name)+tagTime).toString)
      }

      if (!class2List.contains(class2_name)){
        class2List.add(class2_name)
      }
      if (!classImgList.contains(ad_seat_img)){
        classImgList.add(ad_seat_img)
      }

      class_type_id match {
        case "4" =>
          manList.add(class3_name)
          class3Time.add(class3_name + ":" + ad_seat_b_time + "_" + ad_seat_e_time)
          //          man2List.add(file9)
          //          manImgList.add(file10)
        case "1" =>
          objectList.add(class3_name)
          class3Time.add(class3_name + ":" + ad_seat_b_time + "_" + ad_seat_e_time)
          //          object2List.add(file9)
          //          objectImgList.add(file10)
        case "3" =>
          actionList.add(class3_name)
          class3Time.add(class3_name + ":" + ad_seat_b_time + "_" + ad_seat_e_time)
          //          action2List.add(file9)
          //          actionImgList.add(file10)
        case "2" =>
          senceList.add(class3_name)
          class3Time.add(class3_name + ":" + ad_seat_b_time + "_" + ad_seat_e_time)
          //          sence2List.add(file9)
          //          senceImgList.add(file10)
      }

    }

    // 标签按字母顺序排序
    val list = class3List.toArray.toList.sortWith((o1, o2) => {
      o1.toString > o2.toString
    })

    class3List.clear()
    for (i <- list) {
      class3List.add(i)
    }

    temp.put("string_class3_list", class3List)
    temp.put("string_class3", class3List.toString)
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

    temp.put("allTagTime", allTagTime.toString)
    temp.put("class3Time", class3Time.toString)

    var newBT = 0L
    if (minBTime>0){
      newBT = minBTime-3000
    } else {
      newBT = 0
    }

    temp.put("string_time", newBT + "_" + (maxETime))
    temp.put("b_t", minBTime)
    temp.put("e_t", maxETime)
    temp.put("string_time_long", maxETime - newBT)

    temp.put("resourceId", "1")

    val keys = temp.getJSONObject("allTagTime").keySet()
    val ratioObject = new JSONObject
    for (key <- keys.toArray){
      ratioObject.put(key.toString,(temp.getJSONObject("allTagTime").getString(key.toString).toLong/(maxETime - minBTime).toDouble).formatted("%.2f").toDouble)
      ratioObject
    }
    temp.put("tagRatio",ratioObject)
    temp.put("create_time", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date))

    temp
  }


}
