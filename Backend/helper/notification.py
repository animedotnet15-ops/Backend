# ─────────────────────────────────────────────────────────────────────────────
# Author  : ThiruXD
# GitHub  : https://github.com/ThiruXD
# Portfolio: https://thiruxd.is-a.dev
# ─────────────────────────────────────────────────────────────────────────────
from Backend.config import Telegram
from Backend import db
from Backend.pyrofork import StreamBot
from pyrogram.enums import ParseMode
from Backend.logger import LOGGER

async def send_log_report(metadata_info, mode="Automatic"):
    """
    Sends a two-part log report to the configured log channel.
    """
    settings = await db.get_settings()
    log_channel = settings.get("logChannel") or Telegram.REPORT_CHANNEL
    
    if not log_channel:
        LOGGER.warning("No log channel configured for notifications.")
        return

    # Ensure log_channel is an integer if it's a numeric string
    try:
        if isinstance(log_channel, str) and (log_channel.startswith('-') or log_channel.isdigit()):
            log_channel = int(log_channel)
    except Exception:
        pass

    try:
        title = metadata_info.get("title")
        year = metadata_info.get("year")
        rate = metadata_info.get("rate", 0)
        genres = ", ".join(metadata_info.get("genres", []))
        tmdb_id = metadata_info.get("tmdb_id")
        media_type = metadata_info.get("media_type")
        poster = metadata_info.get("poster")
        
        # Determine the link based on media type
        path_type = "mov" if media_type == "movie" else "ser"
        movie_link = f"{Telegram.FRONTEND_LINK}/{path_type}/{tmdb_id}"

        # Message 1: Upload Details
        # For simplicity in 'number of files', we assume 1 since it's per-file addition
        # If it's a bulk link, the caller might need to handle it differently, 
        # but for individual additions, this works.
        msg1 = (
            f"<b>📁 New File Added</b>\n\n"
            f"<b>🆔 ID:</b> <code>{tmdb_id}</code>\n"
            f"<b>📝 Caption:</b> <code>{title}</code>\n"
            f"<b>🚀 Mode:</b> <code>{mode}</code>"
        )
        
        # Message 2: Movie Info
        msg2 = (
            f"<b>🎬 {title} ({year})</b>\n\n"
            f"<b>⭐ Rating:</b> {rate}\n"
            f"<b>🎭 Genres:</b> {genres}\n\n"
            f"<b>🔗 Link:</b> {movie_link}"
        )

        # Send Message 1
        await StreamBot.send_message(
            chat_id=log_channel,
            text=msg1,
            parse_mode=ParseMode.HTML
        )

        # Send Message 2 with photo (poster) if available
        if poster:
            await StreamBot.send_photo(
                chat_id=log_channel,
                photo=poster,
                caption=msg2,
                parse_mode=ParseMode.HTML
            )
        else:
            await StreamBot.send_message(
                chat_id=log_channel,
                text=msg2,
                parse_mode=ParseMode.HTML
            )

    except Exception as e:
        LOGGER.error(f"Error sending log report: {e}")

async def send_bulk_log_report(metadata_info, file_count, mode="Manual Bulk"):
    """
    Sends a summary report for bulk additions.
    """
    settings = await db.get_settings()
    log_channel = settings.get("logChannel") or Telegram.REPORT_CHANNEL
    
    if not log_channel:
        return

    # Ensure log_channel is an integer if it's a numeric string (e.g. -100123...)
    try:
        if isinstance(log_channel, str) and (log_channel.startswith('-') or log_channel.isdigit()):
            log_channel = int(log_channel)
    except Exception:
        pass

    try:
        title = metadata_info.get("title")
        tmdb_id = metadata_info.get("tmdb_id")
        media_type = metadata_info.get("media_type")
        
        path_type = "mov" if media_type == "movie" else "ser"
        movie_link = f"{Telegram.FRONTEND_LINK}/{path_type}/{tmdb_id}"

        msg1 = (
            f"<b>📦 Bulk Addition Complete</b>\n\n"
            f"<b>🔢 Number of Files:</b> <code>{file_count}</code>\n"
            f"<b>🆔 ID:</b> <code>{tmdb_id}</code>\n"
            f"<b>📝 Title:</b> <code>{title}</code>\n"
            f"<b>🚀 Mode:</b> <code>{mode}</code>"
        )
        
        await StreamBot.send_message(
            chat_id=log_channel,
            text=msg1,
            parse_mode=ParseMode.HTML
        )
        
        # We can also send the standard info message
        await send_log_report(metadata_info, mode=mode)

    except Exception as e:
        LOGGER.error(f"Error sending bulk log report: {e}")

async def send_broken_link_report(report: dict):
    """
    Sends a viewer-submitted 'broken link / playback issue' report.
    report = {tmdb_id, media_type, title, issue, message, device_id}

    NOTE: this uses REPORT_CHANNEL specifically (not settings.logChannel),
    so it always goes to the channel set in config.env regardless of what
    the admin panel's "Log Channel" is set to.
    """
    report_channel = Telegram.REPORT_CHANNEL

    if not report_channel:
        LOGGER.warning("No REPORT_CHANNEL configured for broken-link reports.")
        return

    try:
        if isinstance(report_channel, str) and (report_channel.startswith('-') or report_channel.isdigit()):
            report_channel = int(report_channel)
    except Exception:
        pass

    try:
        path_type = "mov" if report.get("media_type") == "movie" else "ser"
        link = f"{Telegram.FRONTEND_LINK}/{path_type}/{report.get('tmdb_id')}"

        text = (
            f"<b>🚩 Broken Link / Playback Report</b>\n\n"
            f"<b>🎬 Title:</b> <code>{report.get('title')}</code>\n"
            f"<b>🆔 TMDB ID:</b> <code>{report.get('tmdb_id')}</code>\n"
            f"<b>⚠️ Issue:</b> <code>{report.get('issue')}</code>\n"
            f"<b>💬 Message:</b> {report.get('message') or '-'}\n"
            f"<b>🔗 Link:</b> {link}"
        )

        await StreamBot.send_message(
            chat_id=report_channel,
            text=text,
            parse_mode=ParseMode.HTML
        )
    except Exception as e:
        LOGGER.error(f"Error sending broken link report: {e}")
        
