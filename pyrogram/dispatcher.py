import asyncio
import inspect
import logging
from collections import OrderedDict

import pyrogram
from pyrogram import raw, types, utils
from pyrogram.handlers.handler import Handler
from pyrogram.handlers import (
    BotBusinessConnectHandler,
    BotBusinessMessageHandler,
    CallbackQueryHandler,
    MessageHandler,
    EditedMessageHandler,
    EditedBotBusinessMessageHandler,
    ErrorHandler,
    DeletedMessagesHandler,
    DeletedBotBusinessMessagesHandler,
    MessageReactionUpdatedHandler,
    MessageReactionCountUpdatedHandler,
    UserStatusHandler,
    RawUpdateHandler,
    InlineQueryHandler,
    PollHandler,
    ShippingQueryHandler,
    PreCheckoutQueryHandler,
    ConversationHandler,
    ChosenInlineResultHandler,
    ChatMemberUpdatedHandler,
    ChatJoinRequestHandler,
    StoryHandler,
    PurchasedPaidMediaHandler
)

from pyrogram.raw.types import (
    UpdateNewMessage, UpdateNewChannelMessage, UpdateNewScheduledMessage,
    UpdateBotBusinessConnect,
    UpdateBotNewBusinessMessage, UpdateBotDeleteBusinessMessage, UpdateBotEditBusinessMessage,
    UpdateEditMessage, UpdateEditChannelMessage,
    UpdateDeleteMessages, UpdateDeleteChannelMessages,
    UpdateBotCallbackQuery, UpdateInlineBotCallbackQuery, UpdateBotPrecheckoutQuery,
    UpdateUserStatus, UpdateBotInlineQuery, UpdateMessagePoll,
    UpdateBotInlineSend, UpdateChatParticipant, UpdateChannelParticipant, UpdateBotStopped,
    UpdateBotChatInviteRequester, UpdateStory,
    UpdateBotMessageReaction,
    UpdateBotMessageReactions,
    UpdateBotShippingQuery,
    UpdateBusinessBotCallbackQuery,
    UpdateBotPurchasedPaidMedia
)


def sort_items(your_dict):
    # 一次遍历分类键
    num_keys = []
    str_keys = []
    for k in your_dict:
        if isinstance(k, (int, float)):
            num_keys.append(k)
        else:
            str_keys.append(k)

    # 排序
    sorted_num_keys = sorted(num_keys)
    sorted_str_keys = sorted(str_keys, key=lambda x: (len(x), x))

    # 创建新字典
    return OrderedDict((k, your_dict[k]) for k in sorted_num_keys + sorted_str_keys)


log = logging.getLogger(__name__)


class Dispatcher:
    NEW_MESSAGE_UPDATES = (UpdateNewMessage, UpdateNewChannelMessage, UpdateNewScheduledMessage)
    NEW_BOT_BUSINESS_MESSAGE_UPDATES = (UpdateBotNewBusinessMessage,)
    EDIT_MESSAGE_UPDATES = (UpdateEditMessage, UpdateEditChannelMessage)
    EDIT_BOT_BUSINESS_MESSAGE_UPDATES = (UpdateBotEditBusinessMessage,)
    DELETE_MESSAGES_UPDATES = (UpdateDeleteMessages, UpdateDeleteChannelMessages)
    DELETE_BOT_BUSINESS_MESSAGES_UPDATES = (UpdateBotDeleteBusinessMessage,)
    CALLBACK_QUERY_UPDATES = (UpdateBotCallbackQuery, UpdateInlineBotCallbackQuery, UpdateBusinessBotCallbackQuery)
    CHAT_MEMBER_UPDATES = (UpdateChatParticipant, UpdateChannelParticipant, UpdateBotStopped,)
    USER_STATUS_UPDATES = (UpdateUserStatus,)
    BOT_INLINE_QUERY_UPDATES = (UpdateBotInlineQuery,)
    POLL_UPDATES = (UpdateMessagePoll,)
    CHOSEN_INLINE_RESULT_UPDATES = (UpdateBotInlineSend,)
    CHAT_JOIN_REQUEST_UPDATES = (UpdateBotChatInviteRequester,)
    NEW_STORY_UPDATES = (UpdateStory,)
    MESSAGE_BOT_NA_REACTION_UPDATES = (UpdateBotMessageReaction,)
    MESSAGE_BOT_A_REACTION_UPDATES = (UpdateBotMessageReactions,)
    BOT_BUSSINESS_CONNECT_UPDATES = (UpdateBotBusinessConnect,)
    PRE_CHECKOUT_QUERY_UPDATES = (UpdateBotPrecheckoutQuery,)
    SHIPPING_QUERY_UPDATES = (UpdateBotShippingQuery,)
    PURCHASED_PAID_MEDIA_UPDATES = (UpdateBotPurchasedPaidMedia,)

    def __init__(self, client: "pyrogram.Client"):
        self.client = client
        self.loop = asyncio.get_event_loop()

        self.handler_worker_tasks = []
        self.locks_list = []
        self.error_handlers = []

        self.updates_queue = asyncio.Queue()
        self.groups = OrderedDict()

        # 会话处理器默认放在组0
        self.conversation_handler = ConversationHandler()
        self.groups[0] = [self.conversation_handler]

        # 可配置参数（通过Client传递）
        self.special_groups = getattr(client, "dispatcher_special_groups", {0, 1, 10})
        self.handler_timeout = getattr(client, "handler_timeout", 60)  # 5分钟默认超时
        self.timeout_tasks = {}  # 管理组的超时任务: {group: task}

        # 更新解析器映射（保持原逻辑）
        async def message_parser(update, users, chats):
            return (
                await pyrogram.types.Message._parse(
                    self.client, update.message, users, chats,
                    is_scheduled=isinstance(update, UpdateNewScheduledMessage)
                ),
                MessageHandler
            )

        async def bot_business_message_parser(update, users, chats):
            return (
                await pyrogram.types.Message._parse(
                    self.client,
                    update.message,
                    users,
                    chats,
                    business_connection_id=update.connection_id
                ),
                BotBusinessMessageHandler
            )

        async def edited_message_parser(update, users, chats):
            parsed, _ = await message_parser(update, users, chats)
            return (parsed, EditedMessageHandler)

        async def edited_bot_business_message_parser(update, users, chats):
            parsed, _ = await bot_business_message_parser(update, users, chats)
            return (parsed, EditedBotBusinessMessageHandler)

        async def deleted_messages_parser(update, users, chats):
            return (
                utils.parse_deleted_messages(self.client, update),
                DeletedMessagesHandler
            )

        async def deleted_bot_business_messages_parser(update, users, chats):
            return (
                utils.parse_deleted_messages(
                    self.client, update, business_connection_id=update.connection_id
                ),
                DeletedBotBusinessMessagesHandler
            )

        async def callback_query_parser(update, users, chats):
            return (
                await pyrogram.types.CallbackQuery._parse(self.client, update, users),
                CallbackQueryHandler
            )

        async def user_status_parser(update, users, chats):
            return (
                pyrogram.types.User._parse_user_status(self.client, update),
                UserStatusHandler
            )

        async def inline_query_parser(update, users, chats):
            return (
                pyrogram.types.InlineQuery._parse(self.client, update, users),
                InlineQueryHandler
            )

        async def poll_parser(update, users, chats):
            return (
                await pyrogram.types.Poll._parse_update(self.client, update, users),
                PollHandler
            )

        async def chosen_inline_result_parser(update, users, chats):
            return (
                pyrogram.types.ChosenInlineResult._parse(self.client, update, users),
                ChosenInlineResultHandler
            )

        async def chat_member_updated_parser(update, users, chats):
            return (
                pyrogram.types.ChatMemberUpdated._parse(self.client, update, users, chats),
                ChatMemberUpdatedHandler
            )

        async def chat_join_request_parser(update, users, chats):
            return (
                pyrogram.types.ChatJoinRequest._parse(self.client, update, users, chats),
                ChatJoinRequestHandler
            )

        async def story_parser(update, users, chats):
            return (
                await pyrogram.types.Story._parse(self.client, update.story, update.peer),
                StoryHandler
            )

        async def shipping_query_parser(update, users, chats):
            return (
                await pyrogram.types.ShippingQuery._parse(self.client, update, users),
                ShippingQueryHandler
            )

        async def pre_checkout_query_parser(update, users, chats):
            return (
                await pyrogram.types.PreCheckoutQuery._parse(self.client, update, users),
                PreCheckoutQueryHandler
            )

        async def message_bot_na_reaction_parser(update, users, chats):
            return (
                pyrogram.types.MessageReactionUpdated._parse(self.client, update, users, chats),
                MessageReactionUpdatedHandler
            )

        async def message_bot_a_reaction_parser(update, users, chats):
            return (
                pyrogram.types.MessageReactionCountUpdated._parse(self.client, update, users, chats),
                MessageReactionCountUpdatedHandler
            )

        async def bot_business_connect_parser(update, users, chats):
            return (
                await pyrogram.types.BotBusinessConnection._parse(self.client, update.connection),
                BotBusinessConnectHandler
            )

        async def purchased_paid_media_parser(update, users, chats):
            return (
                pyrogram.types.PurchasedPaidMedia._parse(self.client, update, users),
                PurchasedPaidMediaHandler
            )

        # 构建更新解析器字典（保持原逻辑）
        self.update_parsers = {
            Dispatcher.NEW_MESSAGE_UPDATES: message_parser,
            Dispatcher.NEW_BOT_BUSINESS_MESSAGE_UPDATES: bot_business_message_parser,
            Dispatcher.EDIT_MESSAGE_UPDATES: edited_message_parser,
            Dispatcher.EDIT_BOT_BUSINESS_MESSAGE_UPDATES: edited_bot_business_message_parser,
            Dispatcher.DELETE_MESSAGES_UPDATES: deleted_messages_parser,
            Dispatcher.DELETE_BOT_BUSINESS_MESSAGES_UPDATES: deleted_bot_business_messages_parser,
            Dispatcher.CALLBACK_QUERY_UPDATES: callback_query_parser,
            Dispatcher.CHAT_MEMBER_UPDATES: chat_member_updated_parser,
            Dispatcher.USER_STATUS_UPDATES: user_status_parser,
            Dispatcher.BOT_INLINE_QUERY_UPDATES: inline_query_parser,
            Dispatcher.POLL_UPDATES: poll_parser,
            Dispatcher.CHOSEN_INLINE_RESULT_UPDATES: chosen_inline_result_parser,
            Dispatcher.CHAT_JOIN_REQUEST_UPDATES: chat_join_request_parser,
            Dispatcher.NEW_STORY_UPDATES: story_parser,
            Dispatcher.SHIPPING_QUERY_UPDATES: shipping_query_parser,
            Dispatcher.PRE_CHECKOUT_QUERY_UPDATES: pre_checkout_query_parser,
            Dispatcher.MESSAGE_BOT_NA_REACTION_UPDATES: message_bot_na_reaction_parser,
            Dispatcher.MESSAGE_BOT_A_REACTION_UPDATES: message_bot_a_reaction_parser,
            Dispatcher.BOT_BUSSINESS_CONNECT_UPDATES: bot_business_connect_parser,
            Dispatcher.PURCHASED_PAID_MEDIA_UPDATES: purchased_paid_media_parser
        }

        # 扁平化解析器映射（保持原逻辑）
        self.update_parsers = {
            key: value
            for key_tuple, value in self.update_parsers.items()
            for key in key_tuple
        }

    async def start(self):
        """启动调度器，创建工作协程"""
        if not self.client.no_updates:
            # 根据配置的工作线程数创建锁和工作任务
            for _ in range(self.client.workers):
                self.locks_list.append(asyncio.Lock())
                self.handler_worker_tasks.append(
                    self.loop.create_task(self.handler_worker(self.locks_list[-1]))
                )

            log.info("Started %s HandlerTasks", self.client.workers)

            # 恢复未处理的更新（保持原逻辑）
            if not self.client.skip_updates:
                await self.client.recover_gaps()

    async def stop(self):
        """停止调度器，清理资源"""
        if not self.client.no_updates:
            # 向工作队列发送终止信号
            for _ in range(self.client.workers):
                self.updates_queue.put_nowait(None)

            # 等待所有工作任务结束
            for task in self.handler_worker_tasks:
                await task

            # 取消所有超时任务并等待结束
            for task in self.timeout_tasks.values():
                task.cancel()
            await asyncio.gather(*self.timeout_tasks.values(), return_exceptions=True)

            # 清理所有状态
            self.handler_worker_tasks.clear()
            self.groups.clear()
            self.error_handlers.clear()
            self.timeout_tasks.clear()
            self.locks_list.clear()

            log.info("Stopped %s HandlerTasks", self.client.workers)

    def add_handler(self, handler, group: int):
        """添加处理器到指定组，非特殊组会自动超时移除"""

        async def fn():
            # 获取所有锁，确保线程安全
            for lock in self.locks_list:
                await lock.acquire()

            try:
                if isinstance(handler, ErrorHandler):
                    # 处理错误处理器（保持原逻辑）
                    if handler not in self.error_handlers:
                        self.error_handlers.append(handler)
                else:
                    # 处理普通处理器
                    if group not in self.groups:
                        self.groups[group] = []
                        self.groups = sort_items(self.groups)  # 保持组有序
                    self.groups[group].append(handler)

                    # 为非特殊组设置超时任务
                    if group not in self.special_groups and 'listen' not in str(group) and 'edit' not in str(group):
                        # 若已有任务则取消（重置超时）
                        if group in self.timeout_tasks:
                            self.timeout_tasks[group].cancel()
                        # 创建新的超时任务
                        task = self.loop.create_task(self._auto_remove_handler(group))
                        self.timeout_tasks[group] = task
            finally:
                # 释放所有锁
                for lock in self.locks_list:
                    lock.release()

        # 异步执行添加逻辑
        self.loop.create_task(fn())

    async def _auto_remove_handler(self, group: int):
        """自动移除超时组的处理器"""
        try:
            # 等待超时时间
            await asyncio.sleep(self.handler_timeout)
            # 移除组及其处理器
            self.remove_all_handler(group)
            log.debug(f"Auto-removed handler group {group} after timeout")
        except asyncio.CancelledError:
            # 任务被主动取消（如手动移除组）
            log.debug(f"Timeout task for group {group} was cancelled")
        except Exception as e:
            log.warning(f"Failed to auto-remove group {group}: {str(e)}")
        finally:
            # 清理任务记录
            if group in self.timeout_tasks:
                del self.timeout_tasks[group]

    def remove_all_handler(self, group: int):
        """移除指定组的所有处理器"""

        async def fn():
            # 获取所有锁
            for lock in self.locks_list:
                await lock.acquire()

            try:
                # 移除组（忽略不存在的组）
                self.groups.pop(group, None)
                # 取消对应的超时任务
                if group in self.timeout_tasks:
                    self.timeout_tasks[group].cancel()
            finally:
                # 释放所有锁
                for lock in self.locks_list:
                    lock.release()

        self.loop.create_task(fn())

    def remove_handler(self, handler, group: int):
        """移除指定组中的特定处理器"""

        async def fn():
            # 获取所有锁
            for lock in self.locks_list:
                await lock.acquire()

            try:
                if isinstance(handler, ErrorHandler):
                    # 处理错误处理器
                    if handler not in self.error_handlers:
                        raise ValueError(f"Error handler {handler} not found")
                    self.error_handlers.remove(handler)
                else:
                    # 处理普通处理器
                    if group not in self.groups:
                        raise ValueError(f"Group {group} not found")
                    self.groups[group].remove(handler)
            finally:
                # 释放所有锁
                for lock in self.locks_list:
                    lock.release()

        self.loop.create_task(fn())

    async def handler_worker(self, lock: asyncio.Lock):
        """处理更新的工作协程"""
        while True:
            packet = await self.updates_queue.get()
            if packet is None:  # 终止信号
                break
            await self._process_packet(packet, lock)

    async def _process_packet(
            self,
            packet: tuple[raw.core.TLObject, dict[int, types.Update], dict[int, types.Update]],
            lock: asyncio.Lock,
    ):
        """处理单个更新包"""
        try:
            update, users, chats = packet
            parser = self.update_parsers.get(type(update))

            # 解析更新（保持原逻辑）
            if parser is not None:
                parsed_result = parser(update, users, chats)
                if inspect.isawaitable(parsed_result):
                    parsed_update, handler_type = await parsed_result
                else:
                    parsed_update, handler_type = parsed_result
            else:
                parsed_update, handler_type = (None, type(None))

            # 执行匹配的处理器
            async with lock:
                for group in self.groups.values():
                    for handler in group:
                        try:
                            if parsed_update is not None:
                                # 检查处理器类型和过滤条件
                                if isinstance(handler, handler_type) and await handler.check(
                                        self.client, parsed_update
                                ):
                                    await self._execute_callback(handler, parsed_update)
                                    break
                            elif isinstance(handler, RawUpdateHandler):
                                # 处理原始更新
                                await self._execute_callback(handler, update, users, chats)
                                break
                        except pyrogram.StopPropagation:
                            # 终止传播
                            raise
                        except pyrogram.ContinuePropagation:
                            # 继续执行下一个处理器
                            continue
                        except Exception as exception:
                            # 处理处理器执行异常
                            if parsed_update is not None:
                                await self._handle_exception(parsed_update, exception)
        except pyrogram.StopPropagation:
            pass
        except Exception as e:
            log.exception("Error processing update: %s", e)
        finally:
            self.updates_queue.task_done()

    async def _handle_exception(self, parsed_update: types.Update, exception: Exception):
        """处理处理器执行中出现的异常"""
        handled_error = False
        for error_handler in self.error_handlers:
            try:
                if await error_handler.check(self.client, parsed_update, exception):
                    handled_error = True
                    break
            except pyrogram.StopPropagation:
                raise
            except pyrogram.ContinuePropagation:
                continue
            except Exception as inner_exception:
                log.exception("Error in error handler: %s", inner_exception)

        if not handled_error:
            log.exception("Unhandled exception: %s", exception)

    async def _execute_callback(self, handler: Handler, *args):
        """执行处理器的回调函数"""
        if inspect.iscoroutinefunction(handler.callback):
            # 异步回调直接执行
            await handler.callback(self.client, *args)
        else:
            # 同步回调在 executor 中执行
            await self.client.loop.run_in_executor(
                self.client.executor, handler.callback, self.client, *args
            )