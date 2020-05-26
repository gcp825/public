#!/usr/bin/python3

def run(cloud=False):
    
#   imports
    
    import apache_beam as ab
    from apache_beam import io
    from apache_beam import ToString as ts
    from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, StandardOptions, GoogleCloudOptions, WorkerOptions
    
    from gcp_tools import beam_tools as bt
    
 #   set up variables and pipeline arguments

    path = 'gs://your-bucket-name/' if cloud else '/local/folder/'
    write_path = path + 'output' + '/'
    
    opt = PipelineOptions(flags=[])  
    
    if cloud:
        opt.view_as(SetupOptions).save_main_session = True
        opt.view_as(SetupOptions).setup_file = './setup.py'
        opt.view_as(StandardOptions).runner = 'DataflowRunner'
#       opt.view_as(WorkerOptions).num_workers = 4                   # best to let dataflow control worker allocation & scaling
        opt.view_as(GoogleCloudOptions).project = 'your-project'
        opt.view_as(GoogleCloudOptions).job_name = 'example-setup'
        opt.view_as(GoogleCloudOptions).staging_location = path + 'temp'
        opt.view_as(GoogleCloudOptions).temp_location = path + 'temp'
        opt.view_as(GoogleCloudOptions).region = 'us-central1'
        
#   define transform functions

    def get_initial(x): return x.strip()[0:1].upper()

#   create data for our main pipeline (may as well use a pipeline for this too)

    with ab.Pipeline(options=opt) as pipe01:

        in01 = (pipe01 | 'Create People'   >> ab.Create(['Mr|Albert|Adams|miss|alberta|adams',
                                                         'Mrs|Christina|Clark|Mr|Christopher|Clark',
                                                         'Dr|Beverly|Brown|||'])
        
                       | 'Write People'    >> io.WriteToText(write_path + 'people.dat',shard_name_template=''))
        
        in02 = (pipe01 | 'Create Lookup'   >> ab.Create(['Mr|Male',
                                                         'Miss|Female',
                                                         'Mrs|Female',
                                                         'Rev|Neutral',
                                                         'Dr|Neutral'])
        
                       | 'Write Lookup'    >> io.WriteToText(write_path + 'lookup.dat',shard_name_template=''))

    if cloud: opt.view_as(GoogleCloudOptions).job_name = 'example-run' # amend job name for 2nd pipeline job
    
    with ab.Pipeline(options=opt) as pipe02:
        
        pc01 = (pipe02 | 'Read Lookup'       >> io.ReadFromText(path + 'lookup.dat', skip_header_lines=0)
                       | 'Lookup ToList'     >> ab.ParDo(bt.ConvertRecTo(list,'|'))
                       | 'Lookup Tuple'      >> bt.CreateLookup(1,0,1))

        pc02 = (pipe02 | 'Read People'       >> io.ReadFromText(path + 'people.dat', skip_header_lines=0)
                       | 'People ToList'     >> ab.ParDo(bt.ConvertRecTo(list,'|'))
                       | 'Normalise'         >> bt.Normalise(2,0,1,2,3,4,5,blanks='n')
                       | 'Transform'         >> bt.XForm(t0=str.title,t1=get_initial,t2=str.upper)
                       | 'Lookup'            >> bt.AppendLookupVals(pc01,0)
                       | 'Rearrange/Drop'    >> bt.KeepFields(1,2,3))
        
        pc03 = (pc02   | 'GroupBy Gender'    >> bt.CreateCountLookup(2)
                       | 'Gender ToList'     >> ab.Map(list)
                       | 'Sort Gender'       >> bt.Sort(1.,0)
                       | 'Format Gender'     >> ts.Iterables(delimiter=': ')
                       | 'Write Gender'      >> io.WriteToText(write_path + 'gender.txt',shard_name_template=''))
 
        pc04 = (pc02   | 'GroupBy Name'      >> bt.CreateCountLookup(1,0)
                       | 'Name ToList'       >> ab.Map(list)
                       | 'Sort Name'         >> bt.Sort(0,1.)
                       | 'Format Name'       >> ts.Iterables(delimiter=': ')
                       | 'Write Name'        >> io.WriteToText(write_path + 'names.txt',shard_name_template=''))
        
if __name__ == '__main__': run()
