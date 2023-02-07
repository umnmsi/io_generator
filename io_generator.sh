#!/bin/bash
#===============================================================================
#
#          FILE: io_generator.sh
# 
#         USAGE: See script_help() or --help
# 
#   DESCRIPTION: Creates arbitrary amounts of data in parallel.  Can be timed
#                as a basic user-space I/O test
# 
#       OPTIONS: See script_help() or --help
#  REQUIREMENTS: GNU Utils
#          BUGS: See GitHub repository
#        AUTHOR: Raychel Benson-Cahoy
#  ORGANIZATION: MSI ASO-PSI
#       CREATED: 2022
#      REVISION: v1.1 (2023-02-03)
#===============================================================================

set -o nounset                              # Treat unset variables as an error
set -o pipefail                             # Return highest code from pipelines

function script_usage () {
  echo "$0 [BUILTIN TEST] [DATA MODIFIERS] [EXTRA TESTS] [--target] TARGET"
  echo ""
  echo "Basic user-space data creation tool / I/O tester"
  echo " 1. Creates TARGET/tmpdir"
  echo " 2. Creates a WIDTH by DEPTH directory structure under TARGET/tmpdir"
  echo " 3. Fills each created directory with COUNT files of BLOCK_SIZE x BLOCK_COUNT in PARALLEL"
  echo " 4. Each file is --block-size x --block-count (data source: /dev/zero)"
  echo " 5. Reports how long that took"
  echo " 6. Syncs all writes to the TARGET filesystem. Reports how long the sync took"
  echo ""
  echo "Optionally mesure (performance testing)"
  echo " a. Path discovery: count and speed of counting (find ... -print)"
  echo " b. Size: Summation of created objects, and speed of summing (du -s ...)"
  echo " c. Deletion: Speed of removing all created objects (find ... -delete)"
  echo "   Speed results via /usr/bin/time --format '%E[lapsed],%S[ystem],%U[ser]"
  echo ""
  echo "Test and creation results reported in 'KEY: Value' format for ingest"
  echo "into visualization backends such as Graphite."
  echo ""
  echo " TARGET/tmpdir is only removed if --delete-test is used or implied"
  echo ""
  echo " EXTRA TESTS"
  echo " --path-test   Count the objects in TARGET_DIR/tmpdir afer generation"
  echo " --size-test    Size the objects in TARGET_DIR/tmpdir after generation"
  echo " --delete-test  Remove (and time) 'tmpdir' after generation"
  echo " --all-tests    Alias for --path-test --size-test --delete-test"
  echo ""
  echo " DATA MODIFIERS"
  echo " --depth|-d DEPTH    Make the directory structure DEPTH deep"
  echo " --width|-w WIDTH    Make each layer of the directory structure WIDTH wide"
  echo " --count|-c COUNT    Fill each directory with COUNT files"
  echo " --block-size|-bs BLOCK_SIZE[KBMGT]"
  echo "                     Write in blocks of BLOCK_SIZE[KBMGT] (see 'man dd: bs=')"
  echo " --block-count|-bc BLOCK_COUNT"
  echo "                     Write BLOCK_COUNT blocks to each file (overrides -br)"
  echo " --block-count-random|-br BLOCK_MIN BLOCK_MAX"
  echo "                     Write between BLOCK_MIN and BLOCK_MAX blocks to each file (overrides -bc)"
  echo ""
  echo " DATA SOURCES"
  echo " --zero       Default.  Read /dev/zero for data to use in write tests"
  echo " --urandom    Read /dev/urandom for data to use in write tests"
  echo "              WARNING: On extremely fast TARGETs, this may be slower"
  echo "              than the TARGET's write speed and cap the write test"
  echo ""
  echo " PREDEFINED TESTS"
  echo " Parameters can be overridden by subsequent DATA MODIFIER arguments"
  echo " Predefined tests DO NO include any optional tests by default"
  echo ""
  echo " --maint-test  Alias for:"
  echo "               --all-tests -d 10 -w 10 -c 25 -bs 1M -br 1 1000 \\"
  echo "               --log [/panfs/roc]/adm/panasas_maint_benchmark_logs/HOSTNAME-DATE.log \\"
  echo "                [/panfs/roc]/scratch/.panasas_maint_benchmark"
  echo "                Space: ~1.6TB total size, 2.7k objects, ~5mn runtime"
  echo ""
  echo "                          -d -w  -c -bs -bc/-br -p  Space  Objects Est. Time"
  echo " --tiny-test               5  5  10  1K    1024  8 ~500MB  ~1k     <1 mn"
  echo " --small-test             10 10 100  1K    1024  8 ~11GB   ~11k    ~1 mn"
  echo " --medium-test            10 10 100  1M   1-100  8 ~500GB  ~11k    ~3 mn"
  echo " --large-test             10 10  25  1M  1-1000  8 ~1.6TB  ~2.7k  ~15 mn"
  echo " --huge-test              10 10 100  1M  1-1000  8 ~5-7TB  ~11k    ~1 hr"
  echo " --ultra-test             10 10 200  1M  1-1000  8 ~10-14T ~22k    ~2 hr"
  echo " --mega-ultra-test        10 10  1K  1M  1-1000  8 ~50-70T ~110k  ~10 hr"
  echo " --tiny-writes-test       10 11 250   1   1-256  8 ~1GB    ~30k    ~1 mn"
  echo " --wide-range-test        10 10 100  1k  1-10^6  8 ~5-7TB  ~11k  5-10 mn"
  echo " --ultra-wide-range-test  10 10 200  1k  1-10^6  8 ~5-7TB  ~11k  5-10 mn"
  echo ""
  echo " TUNING"
  echo " --parallel|-p THREADS  Make # files at a time. Default: $parallel"
  echo ""
  echo " LOGGING"
  echo " --log PATH   Append test results to PATH"
  echo " --header     Include a header in the results (default: off)"
  echo " --key-value  Output results as ',' seperated KEY:VALUE pairs"
  echo ""
  echo " VERBOSITY"
  echo " --verbose     Turn on extra status dialogue about steps and progress"
  echo " --debug       Turn on debug dialogue"
}

# Echo for verbose info
function vecho () { if [ "${verbose:-}" = "true" ]; then echo "$@" >&1; fi ; }; export -f vecho
function decho () { if [ "${debug:-}" = "true" ]; then echo "$@" >&2; fi ; }; export -f decho
function eecho () { echo "Error: $*" >&2 ; }; export -f eecho
function wecho () { echo "Warning: $*" >&2 ; }; export -f wecho

function defaults() {
  depth=5
  width=5
  count=25
  # Block size is set according to acceptable arguments
  #  to the 'bs=' parameter of dd (see man dd for format)
  block_size=1K
  block_count=1024
  # Parallel is NOT set to the value of /proc/cpuinfo like in normal parallel
  # defaults as some network storage systems or their clients could be crippled
  # by a 64 or 128 thread write and result in lower preformance than with a more
  # normal wide thread write that's still probably larger than any single user's
  # point load will be unless their code does highly parallel writes (processing
  # is common, but writes not so much)
  parallel=8
  xargs_print=''

}

function arg_parser () {

  if [ $# -eq 0 ]; then
    script_usage
    exit 0
  fi

  while [ $# -gt 0 ]; do
    case $1 in

      --block-size|-bs)
        block_size=$2
        decho "Block size set to '$block_size'"
        shift 2
        ;;

      --block-count|-bc)
        block_count_random="false"
        block_count=$2
        decho "Block count set to '$block_count'"
        shift 2
        ;;

      --block-count-random|-br)
        block_count_random="true"
        block_count_min="${2}"
        block_count_max="${3}"
        decho "Block random set to '$block_count_random' and min: '$block_count_min' max: '$block_count_max'"
        shift 3
        ;;

      --count|-c)
        count=$2
        decho "Count set to '$count'"
        shift 2
        ;;

      --path-test)
        count_test="true"
        shift
        ;;

      --depth|-d)
        depth=$2
        decho "Depth set to '$depth'"
        shift 2
        ;;

      --debug)
        xargs_print='-t'
        debug='true'
        shift
        ;;

      --log-file|--log)
        log_file="$2"
        if [ -d "${log_file}" ]; then
          # If the log argument is pointed at a DIR then make a unique file handle in the DIR to work with
          if [ ! "${SLURM_JOBID:-}" = '' ]; then
            # If this is a cluster job - use the jobID and hostname to generate the log handle
            log_file="${log_file}/${SLURM_JOBID}-${HOSTNAME}.log"
          else
            # If this is not a cluster job - just use the BASH PID
            log_file="${log_file}/data-gen-$$.log"
          fi
        fi
        decho "Log file set to '${log_file}"
        shift 2
        ;;


      --parallel|-p)
        parallel=$2
        decho "Parallelism set to '$parallel'"
        shift 2
        ;;


      --size-test)
        size_test="true"
        shift
        ;;

      --delete-test)
        delete_test="true"
        shift
        ;;

      --all-tests|--benchmark)
        count_test="true"
        size_test="true"
        delete_test="true"
        shift
        ;;

      --tiny-test)
        # ~500MB, ~1k objects
        block_count_random="false"
        depth=5
        width=5
        count=10
        block_size=1K
        block_count=1024
        parallel=8
        shift
        ;;

      --small-test)
        # ~11GB, ~11k objects
        block_count_random="false"
        depth=10
        width=10
        count=100
        block_size=1K
        block_count=1024
        parallel=8
        shift
        ;;

      --medium-test)
        # ~500 GB of data, ~11k objects
        block_count_random="true"
        depth=10
        width=10
        count=100
        block_size=1M
        block_count_min=1
        block_count_max=100
        parallel=8
        shift
        ;;

      --large-test)
        # ~1.6 TB of data, 2971 objects
        block_count_random="true"
        depth=10
        width=10
        count=25
        block_size=1M
        block_count_min=1
        block_count_max=1000
        parallel=8
        shift
        ;;

      --huge-test)
        # ~5-7 TB of data, ~11k objects
        block_count_random="true"
        depth=10
        width=10
        count=100
        block_size=1M
        block_count_min=1
        block_count_max=1000
        parallel=8
        shift
        ;;

      --ultra-test)
        block_count_random="true"
        depth=10
        width=10
        count=200
        block_size=1M
        block_count_min=1
        block_count_max=1000
        parallel=8
        shift
        ;;

      --mega-ultra-test)
        block_count_random="true"
        depth=10
        width=10
        count=1000
        block_size=1M
        block_count_min=1
        block_count_max=1000
        parallel=8
        shift
        ;;

      --tiny-writes-test)
        # ~1GB, 30k objects
        block_count_random="true"
        depth=10
        width=11
        count=250
        block_size=1
        block_count_min=1
        block_count_max=256
        parallel=8
        shift
        ;;

      --wide-range-test)
        # ~5-7 TB of data, ~11k objects
        block_count_random="true"
        depth=10
        width=10
        count=100
        block_size=1k
        block_count_min=1
        block_count_max=1000000
        parallel=8
        shift
        ;;

      --ultra-wide-range-test)
        block_count_random="true"
        depth=10
        width=10
        count=200
        block_size=1k
        block_count_min=1
        block_count_max=1000000
        parallel=8
        shift
        ;;

      --verbose|-v)
        verbose="true"
        shift
        ;;

      --width|-w)
        width=$2
        decho "Width set to '$width'"
        shift 2
        ;;

      --maint-test)
        #--maint-test|--df-upgrade-test|--maintenance-test)
        # ~1.6 TB of data, ~11k objects
        count_test="true"
        size_test="true"
        delete_test="true"
        block_count_random="true"
        depth=10
        width=10
        count=25
        block_size=1M
        block_count_min=1
        block_count_max=1000
        parallel=8
        # Set the default log file
        hostname="$HOSTNAME"
        if [ "$hostname" = "" ]; then
          hostname="$(ifconfig | grep 'inet ' | grep -v '127.0.0' | awk -F ' ' '{print $2}')"
        fi
        if [ -d '/adm/panasas_maint_benchmark_logs' ]; then
          log_file="/adm/panasas_maint_benchmark_logs/$hostname-$(date +%y-%m-%d_%H:%M).log"
        elif [ -d '/panfs/roc/adm/panasas_maint_benchmark_logs' ]; then
          log_file="/panfs/roc/adm/panasas_maint_benchmark_logs/$hostname-$(date +%y-%m-%d_%H:%M).log"
        else
          wecho "Could not find the normal logging folder for $1 ([/panfs/roc]/adm/panasas_maint_benchmark_logs) - please specify an altranate with --log PATH"
        fi

        # Directory to create under 'origin' depending on which origin is selected
        gen_dir=".panasas_maint_benchmark"

        # Set the default origin
        if [ -d '/panfs/roc/scratch' ]; then
          origin="/panfs/roc/scratch"
        elif [ -d '/scratch' ]; then
          origin="/scratch"
        else
          wecho "Could not find the normal test folder for $1 ([/panfs/roc/]/scratch) - please specify an altranate TARGET_DIR via the normal last option"
        fi
        shift
        ;;

      --target)
        origin="$2"
        shift 2
        ;;

      --help|-h)
        script_usage
        exit 0
        ;;

      *)
        if [ $# -eq 1 ]; then
          origin="$1"
          shift
        else
          eecho "Error: Unrecognized argument $1"
          script_usage
          exit 1
        fi
        ;;
    esac
  done
} 

function sanity_checks_and_setup() {

  # Ensure there's an origin directory provided
  # and that it's a directory AND writable
  if [ -d "${origin:-}" ] && [ -w "${origin:-}" ]; then
    # If there's a 'gen_dir' check that it's present
    if [ ! "${gen_dir:-}" = '' ] && [ ! -d "${origin}/${gen_dir:-}" ]; then
      # If there's a gen_dir but it's not present, make it now
      mkdir "${origin}/${gen_dir}"
      # And update 'origin' to point to the new generated directory
      origin="${origin}/${gen_dir}"
    fi
    # For safety AND the abilty to batch this out and run parallel copies in
    # the same base directory, we want next to make a tempdir here and re-set the "target" to point at this temp_dir
    vecho "Making directory for this run under '$origin' with 'mktemp --tmpdir=\"$origin\" --directory data_generator.XXXXXX'"
    target="$(mktemp --tmpdir="$origin" --directory data_generator.XXXXXX)"
  else
    eecho "Error: Please specify a writable target directory (got '${origin:-}')"
    eecho "$0 [--target] TARGET"
    exit 1
  fi

  if [ ! -d "$target" ]; then
    eecho "Error: Unable to create a temporary directory under '$origin' with 'mktemp'"
    exit 2
  fi
}

function emergency_exit () {
  # Re-trap the kill signal in case we get ctrl-c'd again
  trap exit SIGINT SIGTERM SIGQUIT

  # Wait up to 5 seconds for children to die

  local _wait=5
  wecho "Waiting up to ${_wait}s for child processes to die..."

  # Kill all children of the script so a clanup can happen
  decho "Killing children processes: '$(pgrep -g $$ | grep -v $$)'"
  # NOTE: This is a violation of SC2046 - but we DO want word splitting to happen here
  >/dev/null 2>&1 kill -s 15 $(pgrep -g $$ | grep -v $$)
  # Until the wait counter is '0'
  while [ $_wait -gt 0 ]; do
    # Wait 1 second
    _wait=$(( _wait - 1 ))
    sleep 1
    # If there's no children left, we're done waiting
    if [ $(pgrep -g $$ | wc -l) -eq 1 ]; then
      _wait=0
    fi
    echo "${_wait}s ..."
  done
}; export -f emergency_exit

# Builds out the directory structure defined by WIDTH and DEPTH
function make_dir_structure () {
  vecho "Creating directory structure $depth x $width at $target"
  
  # The depth location starts at '0' and is updated each pass
  local at_depth=0
  local depth_location="$target/depth${at_depth}"
  
  # For each level of depth
  while [ $at_depth -lt $depth ]; do 
    # Make the DEPTH'th directory
    mkdir -p "$depth_location"

    # Reset the width counter
    local at_width=0

    # Make each directory of WIDTH requested at this depth
    while [ $at_width -lt $width ]; do
      mkdir -p "$depth_location/width${at_width}"
      (( at_width++ ))
    done
    
    # Move down one layer of depth
    (( at_depth++ ))
    depth_location="$depth_location/depth${at_depth}"

  done
  vecho "Directory structure created"
}

# fill_dir PATH
# Writes the requested BLOCK_SIZE x BLOCK_COUNT (or random block count) to the
# COUNT number of files in the provided directory
function fill_dir () {

  #trap emergency_exit SIGINT SIGQUIT SIGTERM

  # If the passed argument isn't a directory
  if [ ! -d "${1:-}" ]; then
    eecho "INTERNAL SCRIPT ERROR: directory '${1:-}' does not exist and cannot be filled"
    # Immediately exit
    exit
  fi

  # Set some local handles
  local fill_target="$1"
  local obj_per_dir="$count"

  # If doing random block counts for every file
  if [ "${block_count_random:-}" = "true" ]; then
    vecho "Filling directory '$fill_target' with $count $block_size x $block_count_min -> $block_count_max files"
    # We need to use a read loop here instead of xargs as we can't pass both the 
    while read -r _obj; do
      # Get a random COUNT value between BLOCKS_MIN and BLOCKS_MAX
      local _rand_block_count=$(( (RANDOM % block_count_max ) + block_count_min ))
      # Create the file
      dd if=/dev/zero of="$fill_target/${_obj}-dummy_bs-${block_size}xblks-${_rand_block_count}_file" bs=$block_size count=$_rand_block_count status=none
    done < <(seq -w 0 $obj_per_dir)

  # If doing a static block count for every file
  else
    vecho "Filling directory '$fill_target' with $count $block_size x $block_count files"
    # We can just use XARGS to itterate over the files for us one at a time
    xargs $xargs_print -a <(seq -w 0 $count) --no-run-if-empty -P 1 -I {} \
      dd if=/dev/zero of="$fill_target/{}-dummy_bs-${block_size}xblks-${block_count}_file" bs=$block_size count=$block_count status=none
  fi
}

function count_dir () {
  local count_target="$1"
  vecho "Counting directory '$count_target'"
  local _object_msg="$(/usr/bin/time --format '%E,%U,%S' --output=/dev/stdout --append wc -c < <(find "$count_target" -xdev -type f -print0 | tr -d -c '\0') | tr '\n' ' ')"
  local _object_time="$(<<<$_object_msg cut -d ' ' -f 2)"
  local _object_total="$(<<<$_object_msg cut -d ' ' -f 1)"
  printf 'CountTime: %s\n' "$_object_time"
  printf 'CountResult: %s\n' "$_object_total"
}

function size_dir () {
  local _size_target="$1"
  vecho "Sizing directory '$_size_target'"
  local _size_msg="$(/usr/bin/time --format "%E,%U,%S" --output=/dev/stdout --append du -s "$_size_target" | tr -s '\n' ' ')"
  local _size_time="$(cut -d ' ' -f 3 <<<$_size_msg)"
  local _size_kb="$(cut -d ' ' -f 1 <<<$_size_msg )"
  printf 'SumationTime: %s\n' $_size_time
  printf 'SumationKB: %s\n' $_size_kb
}

function purge_dir () {
  local _purge_target="$1"
  vecho "Purging sub directories of '$_purge_target'"
  /usr/bin/time --format "PurgeTime: %E,%U,%S" --output="${log_file:-/dev/stdout}" --append find "$_purge_target" -xdev -delete
}

function cleanup () {
  # Only cleanup the folder if a remove test was requested
  if [ "${delete_test:-false}" = 'true' ]; then
    if [ -d "$target" ]; then
      find -H "$target"
    fi
  else
    printf "Directory '%s' created with specified object parameters\n" "$target"
  fi
}

function main () {
  # Load defaults
  defaults
  
  # Parse inputs
  arg_parser "$@"

  # Make sure the critical arguments are all present
  # and init the run environment
  sanity_checks_and_setup
  
  # Setup a trap to kill all children processes if needed
  trap emergency_exit SIGINT SIGQUIT SIGTERM
  
  # Init the directory structure
  make_dir_structure

  # Export the functions and variables that the sub-shell is going to need
  export -f fill_dir
  export count block_count block_size block_count_random block_count_min block_count_max log_file

  # We want the "real" time command, not the one provided by whatever shell happens to be running
  #  We also want a parse-able output
  #local time_cmd="/usr/bin/time --format %E,%U,%S"
  vecho "Start time: $(date)"
  vecho "$(uptime)"
  vecho "Parameter: Depth: $depth, Width: $width, Objects per: $count, Block_size: $block_size, Blocks_min: ${block_count_min:-NA}, Blocks_max: ${block_count_max:-$block_count}, Parallel: $parallel, Target: $target"
  
  # Do the write test / data creation
  /usr/bin/time --format="CreationTime: %E,%U,%S" --output="${log_file:-/dev/stdout}" --append \
    xargs -a <(find $target -mindepth 1 -type d -print0) --null --no-run-if-empty -P $parallel -I {} \
    /bin/bash -o nounset -o pipefail -c 'fill_dir "$@"' -- {}
  decho "Debug Post-Parameter: Depth: $depth, Width: $width, Objects per: $count, Block_size: $block_size, Blocks_min: ${block_count_min:-NA}, Blocks_max: ${block_count_max:-$block_count}, Parallel: $parallel, Target: $target"
  vecho "All directories are now filled"

  # There seems to be a 'settling' problem where there's so much parallelism in
  # object creation that objects don't finish getting created before they're
  # ready to be inspected. This sleep is to help mitigate this problem
  vecho "Ensuring writes are syncd"
  /usr/bin/time --format="SyncTime: %E,%U,%S" --output="${log_file:-/dev/stdout}" --append sync -f "$target"
  #cat "$log_file"

  # Do the count test if requested
  if [ "${count_test:-}" = "true" ]; then
    count_dir "$target"
  fi >>"${log_file:-/dev/stdout}"

  # Do the size test if requested
  if [ "${size_test:-}" = "true" ]; then
    size_dir "$target"
  fi >>"${log_file:-/dev/stdout}"

  # Do the delete test if requested
  if [ "${delete_test:-}" = "true" ]; then
    purge_dir "$target"
  fi >>"${log_file:-/dev/stdout}"

  cleanup

}

main "$@"
