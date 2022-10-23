import discord
import pandas as pd

import credentials as cred
from tabulate import tabulate
intents = discord.Intents(messages=True)
client = discord.Client(intents=intents)


@client.event
async def on_ready():
    print(f'{client.user} has connected to Discord!')

best_results_df = pd.read_csv('data/best_results.csv')
best_results_df = best_results_df.loc[:, ['postcode_beginning', 'rental_price', 'living_area', 'living_area_stddev_ratio', 'price_pm', 'price_pm_stddev_ratio', 'rooms', 'energielabel', 'energie_class_stddev_ratio', 'url']]
best_results_df.columns = ['PLZ', '€', 'm²', 'm² diff', '€/m²', '€/m² diff', 'Rooms', 'Energy', 'Energy diff', 'url']

@client.event
async def on_message(message):
    global running
    global observing_changes
    # we do not want the bot to reply to itself
    if message.author == client.user:
        return

    # TODO improve commands: commands lower and upper case working, make list clickable (show recommendations?)
    # TODO put commands inside methods

    if message.content.startswith('!hello'):
        await message.channel.send("Hello")
    if message.content.startswith('!help'):
        await message.channel.send('Run !best to get the best apartments'
                                   '')  # TODO need command for starting/stopping scraper
    if message.content.startswith('!best'):
        channel = message.channel
        for index, eg in best_results_df.head().iterrows():
            embed = discord.Embed(title=f"Apartment in {eg['PLZ']} for {eg['€']}€", url=eg.url)
            embed.add_field(name="€/m²", value=f"{eg['€/m²']:.2f} ({eg['€/m² diff']:+.2f})", inline=True)
            embed.add_field(name="m²", value=f"{eg['m²']:.2f} ({eg['m² diff']:+.2f})", inline=True)
            embed.add_field(name="Energy", value=f"{eg['Energy']} ({eg['Energy diff']:+.2f})", inline=True)
            await channel.send(embed=embed)

def main():
    """
    Initial method once bot starts. Will login to blackboard and retrieve list of courses.
    After that, it will start the discord bot.
    :return: Nothing
    """

    client.run(cred.TOKEN)


if __name__ == '__main__':
    main()
