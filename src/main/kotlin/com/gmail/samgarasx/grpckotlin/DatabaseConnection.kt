package com.gmail.samgarasx.grpckotlin

import org.jetbrains.exposed.sql.Database

const private val URL = "jdbc:postgresql://localhost:5432/<your_database>"
const private val DRIVER = "org.postgresql.Driver"
const private val USER = "<your_user>"
const private val PASSWORD = "<your_password>"

object DatabaseConnection {
    fun connect() {
        Database.connect(URL, DRIVER, USER, PASSWORD)
    }
}