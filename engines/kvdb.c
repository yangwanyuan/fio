/*
 * kvdb engine
 *
 * IO engine using Intel's kvdb.
 *
 */

#include <hlkvds/interface/libckvdb.h>

#include "../fio.h"
#include "../optgroup.h"

struct fio_kvdb_iou {
	struct io_u *io_u;
	kvdb_completion_t completion;
	int io_seen;
	int io_complete;
};

struct kvdb_data {
  kvdb_ioctx_t io;
	struct io_u **aio_events;
	struct io_u **sort_events;
};

struct kvdb_options {
	void *pad;
	char *kvdb_name;
	bool disable_cache;
	bool isSegmented;
	bool isDedup;
	int cache_policy;
	int hash_code;
	char *cache_size;
	//int slru_percent;
	int busy_poll;
};

static struct fio_option options[] = {
	{	
		.name	= "kvdb_name",
		.lname	= "kvdb instance name",
		.type	= FIO_OPT_STR_STORE,
		.off1	= offsetof(struct kvdb_options, kvdb_name),
		.help	= "Different hdcs name will reflect in different cache name",
		.def	= "kvdb",
		.category = FIO_OPT_C_ENGINE,
		.group	= FIO_OPT_G_RBD,
	},
	{       
                .name   = "disable_cache",
		.lname  = "disable read cache",
                .type   = FIO_OPT_BOOL,        
	        .off1   = offsetof(struct kvdb_options, disable_cache), 
                .help   = "kvdb",
                .def    = "kvdb",
                .category = FIO_OPT_C_ENGINE,
                .group  = FIO_OPT_G_RBD,
        },
        {
                .name   = "cache_isSegmented",
                .lname  = "segmened hash table or not",
                .type   = FIO_OPT_BOOL,                .off1   = offsetof(struct kvdb_options, isSegmented),
                .help   = "kvdb",
                .def    = "kvdb",
                .category = FIO_OPT_C_ENGINE,
                .group  = FIO_OPT_G_RBD,
        },
        {       
                .name   = "cache_isDedup",
                .lname  = "dedup or not",
                .type   = FIO_OPT_BOOL,                .off1   = offsetof(struct kvdb_options, isDedup),
                .help   = "kvdb",
                .def    = "kvdb",
                .category = FIO_OPT_C_ENGINE,
                .group  = FIO_OPT_G_RBD,
        },
        {       .name   = "cache_policy",
                .lname  = "read cache policy",
                .type   = FIO_OPT_INT,    
		.off1   = offsetof(struct kvdb_options, cache_policy),
                .help   = "kvdb",
                .category = FIO_OPT_C_ENGINE,
                .group  = FIO_OPT_G_RBD,
        },
        {       .name   = "hash_code",
                .lname  = "hash code if segmented hash table",
                .type   = FIO_OPT_INT,
                .off1   = offsetof(struct kvdb_options, hash_code),
                .help   = "kvdb",
                .category = FIO_OPT_C_ENGINE,
                .group  = FIO_OPT_G_RBD,
        },
        {
                .name   = "cache_size",
                .lname  = "read cache size",
                .type   = FIO_OPT_STR_STORE,
                .off1   = offsetof(struct kvdb_options, cache_size),
                .help   = "kvdb",
                .category = FIO_OPT_C_ENGINE,
                .group  = FIO_OPT_G_RBD,
        },
        /*{       .name   = "slru_percent",
                .lname  = "partition when slru policy",
                .type   = FIO_OPT_INT,   
                .off1   = offsetof(struct kvdb_options, slru_percent),
                .help   = "kvdb",
                .category = FIO_OPT_C_ENGINE,
                .group  = FIO_OPT_G_RBD,
        },*/

};

static int _fio_setup_kvdb_data(struct thread_data *td,
			       struct kvdb_data **kvdb_data_ptr)
{
	struct kvdb_data *kvdb;

	if (td->io_ops->data)
		return 0;

	kvdb = calloc(1, sizeof(struct kvdb_data));
	if (!kvdb)
		goto failed;

	kvdb->aio_events = calloc(td->o.iodepth, sizeof(struct io_u *));
	if (!kvdb->aio_events)
		goto failed;

	kvdb->sort_events = calloc(td->o.iodepth, sizeof(struct io_u *));
	if (!kvdb->sort_events)
		goto failed;

	*kvdb_data_ptr = kvdb;
	return 0;

failed:
	if (kvdb)
		free(kvdb);
	return 1;

}

static int _fio_kvdb_connect(struct thread_data *td)
{
	struct kvdb_data *kvdb = td->io_ops->data;
	struct kvdb_options *o = td->eo;


        char *ptr;
	char *s_input = o->cache_size;
        int segment_size = 0, slru_percent = 0; 
	char *p;
	ptr = strtok_r(s_input, ",", &p);
	if(ptr!=NULL){
		sscanf(ptr, "%d", &segment_size);
	ptr = strtok_r(NULL, ",", &p);
	}
	if(ptr!=NULL){
		sscanf(ptr, "%d", &slru_percent);
	}
	/*if(part!=NULL){
        	sscanf(part, "%d", &segment_size);
		part=strtok(NULL, split);
		if(part!=NULL){
			sscanf(part, "%d", &slru_percent);
		}
	}*/

	kvdb_open(&(kvdb->io), o->kvdb_name, o->disable_cache, o->isSegmented, o->isDedup, o->cache_policy, o->hash_code, segment_size, slru_percent);
	return 0;

}

static void _fio_kvdb_disconnect(struct kvdb_data *kvdb)
{
	if (!kvdb)
		return;

	/* shutdown everything */
  	kvdb_close(kvdb->io);
	//delete (hlkvds::KVDS*)io;
}

static void _fio_kvdb_finish_aiocb(kvdb_completion_t comp, void *data)
{
	struct fio_kvdb_iou *fri = data;
	struct io_u *io_u = fri->io_u;
	ssize_t ret;

	/*
	 * Looks like return value is 0 for success, or < 0 for
	 * a specific error. So we have to assume that it can't do
	 * partial completions.
	 */
	ret = kvdb_aio_get_return_value(fri->completion);
	if (ret < 0) {
		io_u->error = ret;
		io_u->resid = io_u->xfer_buflen;
	} else
		io_u->error = 0;

	fri->io_complete = 1;
}

static struct io_u *fio_kvdb_event(struct thread_data *td, int event)
{
	struct kvdb_data *kvdb = td->io_ops->data;

	return kvdb->aio_events[event];
}

static inline int fri_check_complete(struct kvdb_data *kvdb, struct io_u *io_u,
				     unsigned int *events)
{
	struct fio_kvdb_iou *fri = io_u->engine_data;

	if (fri->io_complete) {
		fri->io_seen = 1;
		kvdb->aio_events[*events] = io_u;
		(*events)++;

		kvdb_aio_release(fri->completion);
		return 1;
	}

	return 0;
}

static inline int kvdb_io_u_seen(struct io_u *io_u)
{
	struct fio_kvdb_iou *fri = io_u->engine_data;

	return fri->io_seen;
}

static void kvdb_io_u_wait_complete(struct io_u *io_u)
{
	struct fio_kvdb_iou *fri = io_u->engine_data;

	kvdb_aio_wait_for_complete(fri->completion);
}

static int kvdb_io_u_cmp(const void *p1, const void *p2)
{
	const struct io_u **a = (const struct io_u **) p1;
	const struct io_u **b = (const struct io_u **) p2;
	uint64_t at, bt;

	at = utime_since_now(&(*a)->start_time);
	bt = utime_since_now(&(*b)->start_time);

	if (at < bt)
		return -1;
	else if (at == bt)
		return 0;
	else
		return 1;
}

static int kvdb_iter_events(struct thread_data *td, unsigned int *events,
			   unsigned int min_evts, int wait)
{
	struct kvdb_data *kvdb = td->io_ops->data;
	unsigned int this_events = 0;
	struct io_u *io_u;
	int i, sidx;

	sidx = 0;
	io_u_qiter(&td->io_u_all, io_u, i) {
		if (!(io_u->flags & IO_U_F_FLIGHT))
			continue;
		if (kvdb_io_u_seen(io_u))
			continue;

		if (fri_check_complete(kvdb, io_u, events))
			this_events++;
		else if (wait)
			kvdb->sort_events[sidx++] = io_u;
	}

	if (!wait || !sidx)
		return this_events;

	/*
	 * Sort events, oldest issue first, then wait on as many as we
	 * need in order of age. If we have enough events, stop waiting,
	 * and just check if any of the older ones are done.
	 */
	if (sidx > 1)
		qsort(kvdb->sort_events, sidx, sizeof(struct io_u *), kvdb_io_u_cmp);

	for (i = 0; i < sidx; i++) {
		io_u = kvdb->sort_events[i];

		if (fri_check_complete(kvdb, io_u, events)) {
			this_events++;
			continue;
		}

		/*
		 * Stop waiting when we have enough, but continue checking
		 * all pending IOs if they are complete.
		 */
		if (*events >= min_evts)
			continue;

		kvdb_io_u_wait_complete(io_u);

		if (fri_check_complete(kvdb, io_u, events))
			this_events++;
	}

	return this_events;
}

static int fio_kvdb_getevents(struct thread_data *td, unsigned int min,
			     unsigned int max, const struct timespec *t)
{
	unsigned int this_events, events = 0;
	struct kvdb_options *o = td->eo;
	int wait = 0;

	do {
		this_events = kvdb_iter_events(td, &events, min, wait);

		if (events >= min)
			break;
		if (this_events)
			continue;

		if (!o->busy_poll)
			wait = 1;
		else
			nop;
	} while (1);

	return events;
}

static int fio_kvdb_queue(struct thread_data *td, struct io_u *io_u)
{
	struct kvdb_data *kvdb = td->io_ops->data;
	struct fio_kvdb_iou *fri = io_u->engine_data;
	int r = -1;

	fio_ro_check(td, io_u);

	fri->io_seen = 0;
	fri->io_complete = 0;

	r = kvdb_aio_create_completion(fri, _fio_kvdb_finish_aiocb, &fri->completion);

	//r = 0;
					
	if (r < 0) {
		log_err("kvdb_aio_create_completion failed.\n");
		goto failed;
	}

	if (io_u->ddir == DDIR_WRITE) {
		r = kvdb_aio_write(kvdb->io, io_u->xfer_buf, io_u->offset, io_u->xfer_buflen, fri->completion);
		fri->io_complete = 1;
		r = 0;
		//_fio_kvdb_finish_aiocb(fri->completion,fri); 
 		/*string get_data;
  		((hlkvds::KVDS *)io)->Get(data, length, get_data);*/
		r = 0;		
		if (r < 0) {
			log_err("kvdb_aio_write failed.\n");
			goto failed_comp;
		}

	} else if (io_u->ddir == DDIR_READ) {
		r = kvdb_aio_read(kvdb->io, io_u->xfer_buf, io_u->offset, io_u->xfer_buflen, fri->completion);
		fri->io_complete = 1;
		r = 0;
		//_fio_kvdb_finish_aiocb(fri->completion,fri);		
		/*((hlkvds::KVDS *)io)->Insert(data, length, data, length);
		r = 0;*/
		if (r < 0) {
			log_err("kvdb_aio_read failed.\n");
			goto failed_comp;
		}
	} else if (io_u->ddir == DDIR_TRIM) {
		/*r = hdcs_aio_discard(io_u->offset,
					io_u->xfer_buflen, fri->completion);
		if (r < 0) {
			log_err("hdcs_aio_discard failed.\n");
			goto failed_comp;
		}*/
	} else if (io_u->ddir == DDIR_SYNC) {
		/*r = hdcs_aio_flush(hdcs->image, fri->completion);
		if (r < 0) {
			log_err("hdcs_flush failed.\n");
			goto failed_comp;
		}*/
	} else {
		dprint(FD_IO, "%s: Warning: unhandled ddir: %d\n", __func__,
		       io_u->ddir);
		goto failed_comp;
	}
	return FIO_Q_QUEUED;
failed_comp:
	kvdb_aio_release(fri->completion);
failed:
	io_u->error = r;
	td_verror(td, io_u->error, "xfer");
	return FIO_Q_COMPLETED;
}

static int fio_kvdb_init(struct thread_data *td)
{
	int r;

	r = _fio_kvdb_connect(td);
	if (r) {
		log_err("fio_hdcs_connect failed, return code: %d .\n", r);
		goto failed;
	}

	return 0;

failed:
	return 1;
}

static void fio_kvdb_cleanup(struct thread_data *td)
{
	struct kvdb_data *kvdb = td->io_ops->data;

	if (kvdb) {
		_fio_kvdb_disconnect(kvdb);
		free(kvdb->aio_events);
		free(kvdb->sort_events);
		free(kvdb);
	}
}

static int fio_kvdb_setup(struct thread_data *td)
{
	//hdcs_image_info_t info;
	struct fio_file *f;
	struct kvdb_data *kvdb = NULL;
	//int major, minor, extra;
	int r;

	/* log version of libhdcs. No cluster connection required. */
	/*hdcs_version(&major, &minor, &extra);
	log_info("hdcs engine: RBD version: %d.%d.%d\n", major, minor, extra);
  */
	/* allocate engine specific structure to deal with libhdcs. */
	r = _fio_setup_kvdb_data(td, &kvdb);
	if (r) {
		log_err("fio_setup_hdcs_data failed.\n");
		goto cleanup;
	}
	td->io_ops->data = kvdb;

	/* libhdcs does not allow us to run first in the main thread and later
	 * in a fork child. It needs to be the same process context all the
	 * time. 
	 */
	td->o.use_thread = 1;

	/* connect in the main thread to determine to determine
	 * the size of the given RADOS block device. And disconnect
	 * later on.
	 */
	/*r = _fio_hdcs_connect(td);
	if (r) {
		log_err("fio_hdcs_connect failed.\n");
		goto cleanup;
	}*/

	/* get size of the RADOS block device */
	/*r = hdcs_stat(hdcs->image, &info, sizeof(info));
	if (r < 0) {
		log_err("hdcs_status failed.\n");
		goto disconnect;
	}
	dprint(FD_IO, "hdcs-engine: image size: %lu\n", info.size);
  */
	/* taken from "net" engine. Pretend we deal with files,
	 * even if we do not have any ideas about files.
	 * The size of the RBD is set instead of a artificial file.
	 */
	if (!td->files_index) {
		add_file(td, td->o.filename ? : "kvdb", 0, 0);
		td->o.nr_files = td->o.nr_files ? : 1;
		td->o.open_files++;
	}
	f = td->files[0];
	f->real_file_size = 1073741824;

	/* disconnect, then we were only connected to determine
	 * the size of the RBD.
	 */
	/*_fio_hdcs_disconnect(hdcs);*/
	return 0;

cleanup:
	fio_kvdb_cleanup(td);
	return r;
}

static int fio_kvdb_open(struct thread_data *td, struct fio_file *f)
{
	return 0;
}

static int fio_kvdb_invalidate(struct thread_data *td, struct fio_file *f)
{
	return 0;
}

static void fio_kvdb_io_u_free(struct thread_data *td, struct io_u *io_u)
{
	struct fio_kvdb_iou *fri = io_u->engine_data;

	if (fri) {
		io_u->engine_data = NULL;
		free(fri);
	}
}

static int fio_kvdb_io_u_init(struct thread_data *td, struct io_u *io_u)
{
	struct fio_kvdb_iou *fri;

	fri = calloc(1, sizeof(*fri));
	fri->io_u = io_u;
	io_u->engine_data = fri;
	return 0;
}

static struct ioengine_ops ioengine = {
	.name			= "kvdb",
	.version		= FIO_IOOPS_VERSION,
	.setup			= fio_kvdb_setup,
	.init			= fio_kvdb_init,
	.queue			= fio_kvdb_queue,
	.getevents		= fio_kvdb_getevents,
	.event			= fio_kvdb_event,
	.cleanup		= fio_kvdb_cleanup,
	.open_file		= fio_kvdb_open,
	.invalidate		= fio_kvdb_invalidate,
	.options		= options,
	.io_u_init		= fio_kvdb_io_u_init,
	.io_u_free		= fio_kvdb_io_u_free,
	.option_struct_size	= sizeof(struct kvdb_options),
};

static void fio_init fio_kvdb_register(void)
{
	register_ioengine(&ioengine);
}

static void fio_exit fio_kvdb_unregister(void)
{
	unregister_ioengine(&ioengine);
}
