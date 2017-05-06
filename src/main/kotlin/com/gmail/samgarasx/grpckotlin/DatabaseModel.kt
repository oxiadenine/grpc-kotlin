package com.gmail.samgarasx.grpckotlin

import org.jetbrains.exposed.sql.Table

object Fruits : Table() {
    val id = integer("id").autoIncrement().primaryKey()
    val no = varchar("no", length = 20).uniqueIndex()
    var description = varchar("description", length = 50).nullable()
}
