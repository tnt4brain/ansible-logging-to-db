# (c) 2012-2014, Michael DeHaan <michael.dehaan@gmail.com>
# (c) 2017 Ansible Project
# (c) 2021 Sergey Pechenko
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)


from __future__ import (absolute_import, division, print_function)

__metaclass__ = type

import pathlib
import sys
import datetime
import warnings
import traceback

DOCUMENTATION = '''
    callback: log2db
    type: stdout
    short_description: default-like Ansible screen output with extended logging support
    version_added: none
    description:
        - This is the default-like output callback for ansible-playbook.
    extends_documentation_fragment:
      - default_callback
    requirements:
      - set as stdout in configuration
    options:
      check_mode_markers:
        name: Show markers when running in check mode
        description:
        - "Toggle to control displaying markers when running in check mode. The markers are C(DRY RUN)
        at the beggining and ending of playbook execution (when calling C(ansible-playbook --check))
        and C(CHECK MODE) as a suffix at every play and task that is run in check mode."
        type: bool
        default: no
        version_added: 2.9
        env:
          - name: ANSIBLE_CHECK_MODE_MARKERS
        ini:
          - key: check_mode_markers
            section: defaults
'''

# TODO: drop here a copy of the defs

# NOTE: check_mode_markers functionality is also implemented in the following derived plugins:
#       debug.py, yaml.py, dense.py. Maybe their documentation needs updating, too.

import ansible.playbook

from ansible import constants as C
from ansible import context
from ansible.playbook.task_include import TaskInclude
from ansible.plugins.callback import CallbackBase
from ansible.utils.color import colorize, hostcolor
from ansible.utils.vars import get_unique_id

from ansible.inventory.host import Host
from ansible.playbook.play import Play
from ansible.playbook.block import Block

from ansible.config.manager import ConfigManager

import json

try:
    from ansible.module_utils import pg8000
except ImportError:
    from ansible.plugins.loader import module_utils_loader

    HAS_PG8K = False
else:
    HAS_PG8K = True

# these are used to provide backwards compat with old plugins that subclass from default
# but still don't use the new config system and/or fail to document the options
# TODO: Change the default of check_mode_markers to True in a future release (2.13)
COMPAT_OPTIONS = (('display_skipped_hosts', C.DISPLAY_SKIPPED_HOSTS),
                  ('display_ok_hosts', True),
                  ('show_custom_stats', C.SHOW_CUSTOM_STATS),
                  ('display_failed_stderr', False),
                  ('check_mode_markers', False),
                  ('show_per_host_start', False))


class CustomJsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if hasattr(self, 'seen'):
            if id(obj) in self.seen:
                # CLOSED LOOP BREAKER
                if hasattr(obj, '_uuid'):
                    return f"{obj._uuid}"
                elif hasattr(obj, 'uuid'):
                    return f"{obj.uuid}"
                else:
                    return f"LOOP BROKEN: {type(obj)}"  # obj.__repr__()
            else:
                self.seen.add(id(obj))
        else:
            self.seen = set()
        if type(obj) in (Host, Play, Block):
            return obj.serialize()
        else:
            return json.JSONEncoder.default(self, obj)


class CallbackModule(CallbackBase):
    '''
    This is the default callback interface, which simply prints messages
    to stdout when new callback events are received.
    '''

    CALLBACK_VERSION = 2.0
    CALLBACK_TYPE = 'stdout'
    CALLBACK_NAME = 'log2db'

    def set_db_config(self, params_dict):
        """Set database connection parameters.

        Fills object property with parameters to connect to PostgreSQL server.

        Args:
            params_dict (dict) -- dictionary with variables

        Kwargs:
            warn_db_default (bool) -- warn that the default DB is used (default True)
        """
        self._display.error(msg=params_dict.__repr__())
        params_dict['password'] = params_dict.pop('pass')
        params_dict['database'] = params_dict.pop('db')
        self._table = params_dict.pop('table')
        if params_dict["host"] == "localhost" and params_dict["socket"] != "":
            params_dict["unix_sock"] = params_dict.pop("socket")

        self._db_opts = {k: v for k, v in params_dict.items() if v}

    def __init__(self, display=None, options=None):
        self._play = None
        self._last_task_banner = None
        self._last_task_name = None
        self._task_type_cache = {}
        super(CallbackModule, self).__init__(display=display, options=options)

        plugin_config_def = {
            'LOG2DB_HOST': {
                'name': 'Host to connect to',
                'default': "localhost",
                'description': [
                    ['This is hostname that will be contacted by plugin']],
                'env': [{"name": "ANSIBLE_LOG2DB_HOST"}],
                "ini": [{"key": "host", "section": "log2db_callback"}],
                "type": "string",
                "version_added": "1.0"},
            'LOG2DB_PORT': {
                'name': 'Host to connect to',
                'default': 0,
                'description': [
                    ['This is target PostgreSQL port']],
                'env': [{"name": "ANSIBLE_LOG2DB_PORT"}],
                "ini": [{"key": "port", "section": "log2db_callback"}],
                "type": "integer",
                "version_added": "1.0"},
            'LOG2DB_USER': {
                'name': 'Account to connect with',
                'default': "ansible",
                'description': [
                    ['This is the user account plugin will use to connect to PostgreSQL']],
                'env': [{"name": "ANSIBLE_LOG2DB_USER"}],
                "ini": [{"key": "user", "section": "log2db_callback"}],
                "type": "string",
                "version_added": "1.0"},
            'LOG2DB_PASS': {
                'name': 'Password to connect with',
                'default': "ansible",
                'description': [
                    ['This is the database account password, huh?...']],
                'env': [{'name': "ANSIBLE_LOG2DB_PASS"}],
                'ini': [{'key': "pass", "section": "log2db_callback"}],
                'type': "string",
                'version_added': "1.0"},
            'LOG2DB_DB': {
                'name': 'Database name',
                'default': 'ansible',
                'description': [
                    ['This is the storage database']],
                'env': [{'name': "ANSIBLE_LOG2DB_DB"}],
                'ini': [{'key': 'database', 'section': "log2db_callback"}],
                'type': 'string',
                'version_added': "1.0"},
            'LOG2DB_TABLE': {
                'name': 'Table name',
                'default': 'logs',
                'description': [
                    ['This is the storage table']],
                'env': [{'name': "ANSIBLE_LOG2DB_TABLE"}],
                'ini': [{'key': 'table', 'section': "log2db_callback"}],
                'type': 'string',
                'version_added': "1.0"},
            'LOG2DB_SOCKET': {
                'name': 'Unix socket for database communication',
                'default': None,
                'description': [
                    ['This is the communication socket']],
                'env': [{'name': "ANSIBLE_LOGD_TABLE"}],
                'ini': [{'key': 'socket', 'section': "log2db_callback"}],
                'type': 'string',
                'version_added': "1.0"},
        }
        self._db_opts = {}
        self._uuid = get_unique_id()
        self._field_list = ('uuid', 'data', 'timestamp', 'origin')
        self._display.b_cowsay = False
        self._load_name = 'log2db'
        self._table = ''

        cm = ConfigManager()
        cm.initialize_plugin_configuration_definitions('callback', 'log2db', plugin_config_def)
        db_config = {k.split('_')[1].lower(): v for k, v in cm.get_plugin_options('callback', 'log2db').items()}
        self.set_db_config(db_config)
        self.set_options()
        if HAS_PG8K is False:
            all_paths = module_utils_loader.print_paths().split(':')
            pth = all_paths[0]
            for i in all_paths:
                if not pathlib.Path(i, 'pg8000').is_dir():
                    continue
                pth = i
                break
            sys.path.append(pth)
            pg8000 = __import__('pg8000')
        if pg8000 is not None:
            self._display.warning(msg=u"PG8K loaded")
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                # self._db_opts['user'],
            self._db_connection = pg8000.connect(**self._db_opts)
            self._db_connection.autocommit = True
        else:
            self._display.warning(msg="Cannot import pg8000 at all - please report an issue")
            self._db_connection = None

    def _single_query(self, info):
        arguments = json.loads((json.dumps(info, check_circular=False, cls=CustomJsonEncoder)))
        origin = arguments.pop('msg_origin')
        query = f"INSERT INTO {self._table} (id, {', '.join(self._field_list)}) VALUES (DEFAULT, {', '.join([':' + x for x in self._field_list])})"
        self._db_connection.run(query,
                                **{'uuid': self._uuid,
                                   'data': arguments,
                                   'timestamp': datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S +0000'),
                                   'origin': origin}
                                )

    def set_options(self, task_keys=None, var_options=None, direct=None):
        super(CallbackModule, self).set_options(task_keys=task_keys, var_options=var_options, direct=direct)

        # for backwards compat with plugins subclassing default, fallback to constants
        for option, constant in COMPAT_OPTIONS:
            try:
                value = self.get_option(option)
            except (AttributeError, KeyError):
                value = constant
            setattr(self, option, value)

    def _serialize_loader(self, d):
        return {'basedir': d._basedir, 'vaults': d._vaults}

    def serialize_playbook(self, p: ansible.playbook.Playbook):
        return {'loader': self._serialize_loader(p.get_loader()),
                'filename': p._file_name, 'basedir': p._basedir,
                'plays': [x.serialize() for x in p.get_plays() if x is Play]}

    def v2_runner_on_failed(self, result, ignore_errors=False):
        self._single_query({**result._result, 'ignore_errors': ignore_errors, 'msg_origin': 'v2_runner_on_failed'})
        delegated_vars = result._result.get('_ansible_delegated_vars', None)
        self._clean_results(result._result, result._task.action)

        if self._last_task_banner != result._task._uuid:
            self._print_task_banner(result._task)

        self._handle_exception(result._result, use_stderr=self.display_failed_stderr)
        self._handle_warnings(result._result)

        if result._task.loop and 'results' in result._result:
            self._process_items(result)

        else:
            if delegated_vars:
                self._display.display(
                    "fatal: [%s -> %s]: FAILED! => %s" % (result._host.get_name(), delegated_vars['ansible_host'],
                                                          self._dump_results(result._result)),
                    color=C.COLOR_ERROR, stderr=self.display_failed_stderr)
            else:
                self._display.display(
                    "fatal: [%s]: FAILED! => %s" % (result._host.get_name(), self._dump_results(result._result)),
                    color=C.COLOR_ERROR, stderr=self.display_failed_stderr)

        if ignore_errors:
            self._display.display("...ignoring", color=C.COLOR_SKIP)

    def v2_runner_on_ok(self, result):
        self._single_query({**result._result, 'msg_origin': 'v2_runner_on_ok'})
        delegated_vars = result._result.get('_ansible_delegated_vars', None)

        if isinstance(result._task, TaskInclude):
            return
        elif result._result.get('changed', False):
            if self._last_task_banner != result._task._uuid:
                self._print_task_banner(result._task)

            if delegated_vars:
                msg = "changed: [%s -> %s]" % (result._host.get_name(), delegated_vars['ansible_host'])
            else:
                msg = "changed: [%s]" % result._host.get_name()
            color = C.COLOR_CHANGED
        else:
            if not self.display_ok_hosts:
                return

            if self._last_task_banner != result._task._uuid:
                self._print_task_banner(result._task)

            if delegated_vars:
                msg = "ok: [%s -> %s]" % (result._host.get_name(), delegated_vars['ansible_host'])
            else:
                msg = "ok: [%s]" % result._host.get_name()
            color = C.COLOR_OK

        self._handle_warnings(result._result)

        if result._task.loop and 'results' in result._result:
            self._process_items(result)
        else:
            self._clean_results(result._result, result._task.action)

            if self._run_is_verbose(result):
                msg += " => %s" % (self._dump_results(result._result),)
            self._display.display(msg, color=color)

    def v2_runner_on_skipped(self, result):
        self._single_query({**result._result, 'msg_origin': 'v2_runner_on_skipped'})
        if self.display_skipped_hosts:

            self._clean_results(result._result, result._task.action)

            if self._last_task_banner != result._task._uuid:
                self._print_task_banner(result._task)

            if result._task.loop and 'results' in result._result:
                self._process_items(result)
            else:
                msg = "skipping: [%s]" % result._host.get_name()
                if self._run_is_verbose(result):
                    msg += " => %s" % self._dump_results(result._result)
                self._display.display(msg, color=C.COLOR_SKIP)

    def v2_runner_on_unreachable(self, result):
        self._single_query({**result._result, 'msg_origin': 'v2_runner_on_unreachable'})
        if self._last_task_banner != result._task._uuid:
            self._print_task_banner(result._task)

        delegated_vars = result._result.get('_ansible_delegated_vars', None)
        if delegated_vars:
            msg = "fatal: [%s -> %s]: UNREACHABLE! => %s" % (
                result._host.get_name(), delegated_vars['ansible_host'], self._dump_results(result._result))
        else:
            msg = "fatal: [%s]: UNREACHABLE! => %s" % (result._host.get_name(), self._dump_results(result._result))
        self._display.display(msg, color=C.COLOR_UNREACHABLE, stderr=self.display_failed_stderr)

    def v2_playbook_on_no_hosts_matched(self):
        self._single_query({'msg_origin': 'v2_playbook_on_no_hosts_matched'})
        self._display.display("skipping: no hosts matched", color=C.COLOR_SKIP)

    def v2_playbook_on_no_hosts_remaining(self):
        self._single_query({'msg_origin': 'v2_playbook_on_no_hosts_remaining'})
        self._display.banner("NO MORE HOSTS LEFT")

    def v2_playbook_on_task_start(self, task, is_conditional):
        self._single_query(
            {**task.serialize(), 'is_conditional': is_conditional, 'msg_origin': 'v2_playbook_on_task_start'})
        self._task_start(task, prefix='TASK')

    def _task_start(self, task, prefix=None):
        # TODO understand if this is needed self._single_query(**task.serialize(), prefix=prefix, msg_origin='')
        # Cache output prefix for task if provided
        # This is needed to properly display 'RUNNING HANDLER' and similar
        # when hiding skipped/ok task results
        if prefix is not None:
            self._task_type_cache[task._uuid] = prefix

        # Preserve task name, as all vars may not be available for templating
        # when we need it later
        if self._play.strategy == 'free':
            # Explicitly set to None for strategy 'free' to account for any cached
            # task title from a previous non-free play
            self._last_task_name = None
        else:
            self._last_task_name = task.get_name().strip()

            # Display the task banner immediately if we're not doing any filtering based on task result
            if self.display_skipped_hosts and self.display_ok_hosts:
                self._print_task_banner(task)

    def _print_task_banner(self, task):
        # args can be specified as no_log in several places: in the task or in
        # the argument spec.  We can check whether the task is no_log but the
        # argument spec can't be because that is only run on the target
        # machine and we haven't run it thereyet at this time.
        #
        # So we give people a config option to affect display of the args so
        # that they can secure this if they feel that their stdout is insecure
        # (shoulder surfing, logging stdout straight to a file, etc).
        args = ''
        if not task.no_log and C.DISPLAY_ARGS_TO_STDOUT:
            args = u', '.join(u'%s=%s' % a for a in task.args.items())
            args = u' %s' % args

        prefix = self._task_type_cache.get(task._uuid, 'TASK')

        # Use cached task name
        task_name = self._last_task_name
        if task_name is None:
            task_name = task.get_name().strip()

        if task.check_mode and self.check_mode_markers:
            checkmsg = " [CHECK MODE]"
        else:
            checkmsg = ""
        self._display.banner(u"%s [%s%s]%s" % (prefix, task_name, args, checkmsg))
        if self._display.verbosity >= 2:
            path = task.get_path()
            if path:
                self._display.display(u"task path: %s" % path, color=C.COLOR_DEBUG)

        self._last_task_banner = task._uuid

    def v2_playbook_on_cleanup_task_start(self, task):
        self._single_query({**task.serialize(), 'msg_origin': 'v2_playbook_on_cleanup_task_start'})
        self._task_start(task, prefix='CLEANUP TASK')

    def v2_playbook_on_haqndler_task_start(self, task):
        self._single_query({**task.serialize(), 'msg_origin': 'v2_playbook_on_handler_task_start'})
        self._task_start(task, prefix='RUNNING HANDLER')

    def v2_runner_on_start(self, host, task):
        self._single_query({'task': task.serialize(), 'host': host.serialize(), 'msg_origin': 'v2_runner_on_start'})
        if self.get_option('show_per_host_start'):
            self._display.display(" [started %s on %s]" % (task, host), color=C.COLOR_OK)

    def v2_playbook_on_play_start(self, play):
        tmp = {**play.serialize()}.update({'msg_origin': 'v2_playbook_on_play_start'})
        self._single_query(tmp)
        name = play.get_name().strip()
        if play.check_mode and self.check_mode_markers:
            checkmsg = " [CHECK MODE]"
        else:
            checkmsg = ""
        if not name:
            msg = u"PLAY%s" % checkmsg
        else:
            msg = u"PLAY [%s]%s" % (name, checkmsg)

        self._play = play

        self._display.banner(msg)

    def v2_on_file_diff(self, result):
        self._single_query({**result._result, 'msg_origin': 'v2_on_file_diff'})
        if result._task.loop and 'results' in result._result:
            for res in result._result['results']:
                if 'diff' in res and res['diff'] and res.get('changed', False):
                    diff = self._get_diff(res['diff'])
                    if diff:
                        if self._last_task_banner != result._task._uuid:
                            self._print_task_banner(result._task)
                        self._display.display(diff)
        elif 'diff' in result._result and result._result['diff'] and result._result.get('changed', False):
            diff = self._get_diff(result._result['diff'])
            if diff:
                if self._last_task_banner != result._task._uuid:
                    self._print_task_banner(result._task)
                self._display.display(diff)

    def v2_runner_item_on_ok(self, result):
        self._single_query({**result._result, 'msg_origin': 'v2_runner_item_on_ok'})
        delegated_vars = result._result.get('_ansible_delegated_vars', None)
        if isinstance(result._task, TaskInclude):
            return
        elif result._result.get('changed', False):
            if self._last_task_banner != result._task._uuid:
                self._print_task_banner(result._task)

            msg = 'changed'
            color = C.COLOR_CHANGED
        else:
            if not self.display_ok_hosts:
                return

            if self._last_task_banner != result._task._uuid:
                self._print_task_banner(result._task)

            msg = 'ok'
            color = C.COLOR_OK

        if delegated_vars:
            msg += ": [%s -> %s]" % (result._host.get_name(), delegated_vars['ansible_host'])
        else:
            msg += ": [%s]" % result._host.get_name()

        msg += " => (item=%s)" % (self._get_item_label(result._result),)

        self._clean_results(result._result, result._task.action)
        if self._run_is_verbose(result):
            msg += " => %s" % self._dump_results(result._result)
        self._display.display(msg, color=color)

    def v2_runner_item_on_failed(self, result):
        self._single_query({**result._result, 'msg_origin': 'v2_runner_item_on_failed'})
        if self._last_task_banner != result._task._uuid:
            self._print_task_banner(result._task)

        delegated_vars = result._result.get('_ansible_delegated_vars', None)
        self._clean_results(result._result, result._task.action)
        self._handle_exception(result._result)

        msg = "failed: "
        if delegated_vars:
            msg += "[%s -> %s]" % (result._host.get_name(), delegated_vars['ansible_host'])
        else:
            msg += "[%s]" % (result._host.get_name())

        self._handle_warnings(result._result)
        self._display.display(
            msg + " (item=%s) => %s" % (self._get_item_label(result._result), self._dump_results(result._result)),
            color=C.COLOR_ERROR)

    def v2_runner_item_on_skipped(self, result):
        self._single_query({**result._result, 'msg_origin': 'v2_runner_item_on_skipped'})
        if self.display_skipped_hosts:
            if self._last_task_banner != result._task._uuid:
                self._print_task_banner(result._task)

            self._clean_results(result._result, result._task.action)
            msg = "skipping: [%s] => (item=%s) " % (result._host.get_name(), self._get_item_label(result._result))
            if self._run_is_verbose(result):
                msg += " => %s" % self._dump_results(result._result)
            self._display.display(msg, color=C.COLOR_SKIP)

    def v2_playbook_on_include(self, included_file):
        self._single_query({**included_file, 'msg_origin': 'v2_playbook_on_include'})
        msg = 'included: %s for %s' % (included_file._filename, ", ".join([h.name for h in included_file._hosts]))
        if 'item' in included_file._args:
            msg += " => (item=%s)" % (self._get_item_label(included_file._args),)
        self._display.display(msg, color=C.COLOR_SKIP)

    def v2_playbook_on_stats(self, stats):
        self._display.banner("PLAY RECAP")

        hosts = sorted(stats.processed.keys())
        for h in hosts:
            t = stats.summarize(h)

            self._display.display(
                u"%s : %s %s %s %s %s %s %s" % (
                    hostcolor(h, t),
                    colorize(u'ok', t['ok'], C.COLOR_OK),
                    colorize(u'changed', t['changed'], C.COLOR_CHANGED),
                    colorize(u'unreachable', t['unreachable'], C.COLOR_UNREACHABLE),
                    colorize(u'failed', t['failures'], C.COLOR_ERROR),
                    colorize(u'skipped', t['skipped'], C.COLOR_SKIP),
                    colorize(u'rescued', t['rescued'], C.COLOR_OK),
                    colorize(u'ignored', t['ignored'], C.COLOR_WARN),
                ),
                screen_only=True
            )

            self._display.display(
                u"%s : %s %s %s %s %s %s %s" % (
                    hostcolor(h, t, False),
                    colorize(u'ok', t['ok'], None),
                    colorize(u'changed', t['changed'], None),
                    colorize(u'unreachable', t['unreachable'], None),
                    colorize(u'failed', t['failures'], None),
                    colorize(u'skipped', t['skipped'], None),
                    colorize(u'rescued', t['rescued'], None),
                    colorize(u'ignored', t['ignored'], None),
                ),
                log_only=True
            )
            self._single_query({**{h: {'ok': t['ok'],
                                       'changed': t['changed'],
                                       'unreachable': t['unreachable'],
                                       'failed': t['failures'],
                                       'skipped': t['skipped'],
                                       'rescued': t['rescued'],
                                       'ignored': t['ignored']}}, 'msg_origin': 'v2_playbook_on_stats'})

        self._display.display("", screen_only=True)

        # print custom stats if required
        if stats.custom and self.show_custom_stats:
            self._display.banner("CUSTOM STATS: ")
            # per host
            # TODO: come up with 'pretty format'
            for k in sorted(stats.custom.keys()):
                if k == '_run':
                    continue
                self._display.display(
                    '\t%s: %s' % (k, self._dump_results(stats.custom[k], indent=1).replace('\n', '')))

            # print per run custom stats
            if '_run' in stats.custom:
                self._display.display("", screen_only=True)
                self._display.display(
                    '\tRUN: %s' % self._dump_results(stats.custom['_run'], indent=1).replace('\n', ''))
            self._display.display("", screen_only=True)

        if context.CLIARGS['check'] and self.check_mode_markers:
            self._display.banner("DRY RUN")

    def v2_playbook_on_start(self, playbook):
        self._single_query({**self.serialize_playbook(playbook), 'msg_origin': 'v2_playbook_on_start'})
        if self._display.verbosity > 1:
            from os.path import basename
            self._display.banner("PLAYBOOK: %s" % basename(playbook._file_name))

        # show CLI arguments
        if self._display.verbosity > 3:
            if context.CLIARGS.get('args'):
                self._display.display('Positional arguments: %s' % ' '.join(context.CLIARGS['args']),
                                      color=C.COLOR_VERBOSE, screen_only=True)

            for argument in (a for a in context.CLIARGS if a != 'args'):
                val = context.CLIARGS[argument]
                if val:
                    self._display.display('%s: %s' % (argument, val), color=C.COLOR_VERBOSE, screen_only=True)

        if context.CLIARGS['check'] and self.check_mode_markers:
            self._display.banner("DRY RUN")

    def v2_runner_retry(self, result):
        self._single_query({**result._result, 'msg_origin': 'v2_runner_retry'})
        task_name = result.task_name or result._task
        msg = "FAILED - RETRYING: %s (%d retries left)." % (
            task_name, result._result['retries'] - result._result['attempts'])
        if self._run_is_verbose(result, verbosity=2):
            msg += "Result was: %s" % self._dump_results(result._result)
        self._display.display(msg, color=C.COLOR_DEBUG)

    def v2_playbook_on_notify(self, handler, host):
        self._single_query({**handler, 'msg_origin': 'v2_playbook_on_notify'})
        if self._display.verbosity > 1:
            self._display.display("NOTIFIED HANDLER %s for %s" % (handler.get_name(), host), color=C.COLOR_VERBOSE,
                                  screen_only=True)
