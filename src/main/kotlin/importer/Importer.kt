package io.github.clemenscode.eventliquefier.importer

import javax.annotation.PostConstruct
import javax.annotation.PreDestroy

abstract class Importer(jobCount: Int = 1, jobCheckIntervalSec: Long = 300L) {

    private val jobManager = JobManager(this::class.java.simpleName, jobCount, jobCheckIntervalSec, ::import)

    @PostConstruct
    fun init() {
        jobManager.startJobCheck()
    }

    @PreDestroy
    fun tearDown() {
        jobManager.tearDown()
    }

    abstract suspend fun import()

}
