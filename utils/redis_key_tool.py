from function.context import context_var


def build_redis_key(unique_prefix, params):
    if not params:
        raise Exception("参数为空!")
    # ab
    if context_var.get() and context_var.get().BUCKET:
        bucket = "-".join(sorted(context_var.get().BUCKET))
    else:
        bucket = ''
    params['unique_prefix'] = unique_prefix
    params['bucket'] = bucket
    cache_key = ":".join(str(k) + "-" + str(v) for k, v in params.items())
    return cache_key

def buildBucketParams(params):
    if context_var.get() and context_var.get().BUCKET:
        bucket = "-".join(sorted(context_var.get().BUCKET))
    else:
        bucket = ''
    params['bucket'] = bucket
    return params
