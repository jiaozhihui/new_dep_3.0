package com.bjvca.filmedit

case class Template(
                     label_id: String,
                     label: Label,
                     resolution: String,
                     frame: String,
                     totalLong: String,
                     seat_num: Int
                   )

/**
 *
 * label_id: String             模板id
 * one: Label                   标签位1
 * two: Label                   标签位2
 * three: Label                 标签位3
 * four: Label                  标签位4
 * five: Label                  标签位5
 * six: Label                   标签位6
 * resolution: String           分辨率
 * frame：Int                    帧数
 * totalLong: String            总时长
 *
 */