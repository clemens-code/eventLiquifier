package io.github.clemenscode.eventliquefier.importer

import io.github.clemenscode.eventliquefier.utils.getLogger
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import java.time.Duration
import java.util.concurrent.ConcurrentLinkedQueue
import kotlin.coroutines.cancellation.CancellationException

/**
 * Capsules the logic of the Job Checker for a task.
 */
internal class JobManager(
        private val jobName: String,
        private val jobAmount: Int,
        private val checkIntervalSec: Long,
        private val task: suspend () -> Unit,
        dispatcher: CoroutineDispatcher = Dispatchers.Default
) {

    private val logger = getLogger(JobManager::class.java)

    private val jobs = ConcurrentLinkedQueue<Job>()

    // Children of a supervisor job can fail independently of each other.
    private val scope = CoroutineScope(dispatcher + SupervisorJob())

    fun tearDown() {
        jobs.forEach {
            it.cancel()
        }
    }

    fun startJobCheck() {
        logger.info("Starting {} Jobs", jobName)
        scope.launch {
            while (isActive) {
                try {
                    checkRunningJobs()
                    delay(Duration.ofSeconds(checkIntervalSec).toMillis())
                } catch (_: CancellationException) {
                    logger.error("Check of tasks of {} was cancelled!", jobName)
                } catch (e: Throwable) {
                    logger.error("Check of tasks of {} failed!", jobName, e)
                    startJobCheck()
                }
            }
        }
    }

    private fun checkRunningJobs() {
        abortInactiveJobs()
        logger.info("Checking Jobs of {}", jobName)
        runBlocking {
            createMissingJobs()
        }
        logger.info("Checked missing Jobs of {}.", jobName)
    }

    private fun abortInactiveJobs() {
        jobs.filterNot { it.isActive }.forEach {
            it.cancel()
            jobs.remove(it)
        }
    }

    private fun createMissingJobs() {
        repeat(jobAmount - jobs.count(Job::isActive)) {
            logger.info("Checking Jobs: {} of {} jobs of {} are active", it, jobAmount, jobName)
            jobs.add(scope.launchTask())
        }
    }

    private fun CoroutineScope.launchTask() =
            launch {
                while (isActive) {
                    try {
                        task()
                    } catch (e: CancellationException) {
                        logger.info("Launch of task for {} was cancelled!", jobName, e)
                    } catch (e: Throwable) {
                        logger.error("Launch of task for {} failed!", jobName, e)
                    }
                }
            }


}
