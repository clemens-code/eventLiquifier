package io.github.clemenscode.eventliquefier.utils

import org.slf4j.Logger
import org.slf4j.LoggerFactory

fun getLogger(clazz: Class<out Any>) = LoggerFactory.getLogger(clazz) ?: error("Could not get Logger")
