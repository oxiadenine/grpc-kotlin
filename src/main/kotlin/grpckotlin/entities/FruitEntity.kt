package grpckotlin.entities

import org.jetbrains.exposed.dao.EntityID
import org.jetbrains.exposed.dao.LongEntity
import org.jetbrains.exposed.dao.LongEntityClass
import org.jetbrains.exposed.dao.LongIdTable

object Fruits : LongIdTable("fruit") {
    val no = Fruits.varchar("no", length = 20).uniqueIndex()
    var description = Fruits.varchar("description", length = 50)
}

class FruitEntity(id: EntityID<Long>) : LongEntity(id) {
    companion object : LongEntityClass<FruitEntity>(Fruits)

    var no by Fruits.no
    var description by Fruits.description
}
