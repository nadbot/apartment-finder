import asyncio
from datetime import datetime, timedelta

import discord
import pandas as pd

import credentials as cred
from pipeline import pipeline

intents = discord.Intents(messages=True)
client = discord.Client(intents=intents)
running = False
next_observation = datetime.utcnow()
starttime = datetime.utcnow()
background_task_time = 60
threshold_good_apartment = 2


@client.event
async def on_ready():
    print(f'{client.user} has connected to Discord!')


best_results_df = pd.read_csv('data/best_results.csv')
best_results_df = best_results_df.loc[:,
                  ['postcode_beginning', 'rental_price', 'living_area', 'living_area_stddev_ratio', 'price_pm',
                   'price_pm_stddev_ratio', 'rooms', 'energielabel', 'energie_class_stddev_ratio', 'url']]


@client.event
async def on_message(message):
    global running
    global background_task_time
    global threshold_good_apartment
    # we do not want the bot to reply to itself
    if message.author == client.user:
        return

    # TODO improve commands: commands lower and upper case working, make list clickable (show recommendations?)
    # TODO put commands inside methods
    channel = message.channel
    if message.content.startswith('!hello'):
        await message.channel.send("Hello")
    if message.content.startswith('!help'):
        await message.channel.send('Run !best to get the best apartments'
                                   '')  # TODO need command for starting/stopping scraper
    if message.content.startswith('!best'):
        await channel.send('Here are the 5 best apartments in Utrecht:')
        await send_new_apartments(channel, best_results_df)
    if message.content.startswith('!status'):
        uptime = runtime()
        if running:
            remaining = time_left()
            status = "Currently observing changes. Use !stop to stop this." + '\n' + remaining
        else:
            status = "Currently not observing any changes. Use !find to observe them."
        await message.channel.send(uptime + "\n" + status)
    if message.content.startswith('!find'):
        # item will be observed, bot will send message once grade becomes available, checking every few minutes
        await channel.send("Finding new apartments")
        # prevents it from running multiple instances at once
        if not running:
            client.loop.create_task(my_background_task(channel, background_task_time))
            running = True
    if message.content.startswith('!stop'):
        running = False
        await message.channel.send("Observation has been stopped.")
    if message.content.startswith('!setTimeout'):
        timeout = message.content.split(' ')[1]
        print(timeout)
        background_task_time = int(timeout)
        await message.channel.send(f"Successfully changed the timeout between checking to {background_task_time}")
    if message.content.startswith('!setThreshold'):
        threshold = message.content.split(' ')[1]
        print(threshold)
        threshold_good_apartment = float(threshold)
        await message.channel.send(
            f"Successfully updated the threshold for accepting an apartment as good to {threshold_good_apartment}")


async def send_new_apartments(channel, df):
    for index, eg in df.head().iterrows():
        embed = discord.Embed(title=f"Apartment in {eg['postcode_beginning']} for {eg['rental_price']}€", url=eg.url)
        embed.add_field(name="€/m²", value=f"{eg['price_pm']:.2f} ({eg['price_pm_stddev_ratio']:+.2f})", inline=True)
        embed.add_field(name="m²", value=f"{eg['living_area']:.2f} ({eg['living_area_stddev_ratio']:+.2f})",
                        inline=True)
        embed.add_field(name="Energy", value=f"{eg['energielabel']} ({eg['energie_class_stddev_ratio']:+.2f})",
                        inline=True)
        await channel.send(embed=embed)


async def my_background_task(channel, time=180):
    """
    Asynchronous method used to check regularly for updates on specific grades.
    :param channel: Channel on which update should be sent
    :param time: Time in seconds how often the background task should run
    :return: returns nothing, prints to user once it finds something
    """
    while running:
        # TODO run pipeline in different thread to not block discord input
        df = pipeline(threshold_good_apartment)
        # check for updates
        if df is not None and not df.empty:
            # send_new_apartments(channel, df)
            await send_new_apartments(channel, df)
        else:
            print('Found no new apartments')
        # otherwise try again in 3 minutes
        global next_observation
        print("Running Async task after " + str(time) + " seconds")
        next_observation = datetime.utcnow() + timedelta(seconds=time)
        await asyncio.sleep(time)  # task runs every 180 seconds


def runtime():
    now = datetime.utcnow()
    elapsed = now - starttime
    seconds = elapsed.seconds
    minutes, seconds = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    return "Running for {}d {}h {}m {}s".format(elapsed.days, hours, minutes, seconds)


def time_left():
    now = datetime.utcnow()
    elapsed = next_observation - now
    seconds = elapsed.seconds
    minutes, seconds = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    return "Next observation will be done in {}d {}h {}m {}s".format(elapsed.days, hours, minutes, seconds)


def main():
    """
    Initial method once bot starts. Will login to blackboard and retrieve list of courses.
    After that, it will start the discord bot.
    :return: Nothing
    """
    client.run(cred.TOKEN)


if __name__ == '__main__':
    main()
