import sys
import os
import json
from os import path
import pandas as pd
from datetime import datetime
import plotly
import plotly.graph_objects as go
import math


# création d'une fonction qui extrait le timestamp et en fait un objet date,
# et qui extrait le nom du profil/de la page et le convertit dans le bon format,
# et qui extrait l'URL
def transform1(df):
    df['timestamp'] = df.apply(lambda x: datetime.fromtimestamp(x['entries']['timestamp']), axis=1)
    df['name'] = df.apply(lambda x: x['entries']['data']['name'].encode('latin1').decode('utf8'), axis=1)
    df['URL'] = df.apply(lambda x: x['entries']['data']['uri'], axis=1)
    return df.drop(columns=['entries', 'description'])

def transform2(df) :
    df['start_timestamp'] = df.apply(lambda x : datetime.fromtimestamp(x['start_timestamp']), axis = 1)
    df['end_timestamp'] = df.apply(lambda x : datetime.fromtimestamp(x['end_timestamp']), axis = 1)
    df['timestamp'] = df['start_timestamp']
    df['name'] = df.apply(lambda x : x['name'].encode('latin1').decode('utf8'), axis = 1)
    return df

def transform(df, key):
    df['timestamp'] = df[key].apply(lambda x : datetime.fromtimestamp(x['timestamp']))
    df['name'] = df[key].apply(lambda x : x['name'].encode('latin1').decode('utf8'))
    df = df.drop(columns = [key])
    return df

#concatène les éléments d'une liste en string séparé avec une virgule
#flag à True si la liste contient + de 2 éléments
def aggregate_liste(l):
    s = ''
    flag = False
    n = len(l)
    if n > 2 :
        flag = True
    for x in range(0, n-1):
        s = s + str(l[x]) + ', '
    s += str(l[n-1])
    return [s, flag]


if __name__ == '__main__':

    #load the foler name, and exit if no name inputted
    try :
        folder_name = str(sys.argv[1])
    except :
        print('You failed to provide a folder name as input', file =sys.stderr)
        sys.exit(1)

    #check if folder name is valid and exit if not
    if not path.exists(folder_name):
        print('The folder name inputted does not exist', file = sys.stderr)
        sys.exit(1)

    try:
        preferences = pd.read_json(folder_name + '/about_you/preferences.json')
    except:
        print('about_you/preferences.json does not exist', file=sys.stderr)
        preferences = None

    voir_en_premier = None
    voir_moins = None
    contacts_bloques = None
    notifs_pages = None

    #préférences est une nested structure, chaque clé est une structure d'intérêt. On en extrait un subset identifié
    #comme pertinent
    if preferences is not None:
        for item in preferences['preferences']:
            if item['name'].encode('latin1').decode('utf8') == 'Voir en premier':
                voir_en_premier = pd.DataFrame(item)
            elif item['name'].encode('latin1').decode('utf8') == 'Voir moins':
                voir_moins = pd.DataFrame(item)
            elif item['name'].encode('latin1').decode('utf8') == 'Contacts Messenger que vous avez bloqués':
                contacts_bloques = pd.DataFrame(item)
            elif item['name'].encode('latin1').decode('utf8') == 'Notifications de Pages':
                notifs_pages = pd.DataFrame(item)
            else:
                pass

    if voir_en_premier is not None:
        voir_en_premier = transform1(voir_en_premier)
    if voir_moins is not None:
        voir_moins = transform1(voir_moins)
    if contacts_bloques is not None:
        contacts_bloques = transform1(contacts_bloques)
    if notifs_pages is not None:
        notifs_pages = transform1(notifs_pages)

    try:
        visited = pd.read_json(folder_name + '/about_you/visited.json')
    except:
        print('about_you/visited.json does not exist', file=sys.stderr)
        visited = None
    visites_de_profil = None
    visites_sur_la_page = None
    events_visited = None
    groups_visited = None

    if visited is not None:
        for item in visited['visited_things']:
            if item['name'] == 'Visites de profil':
                visites_de_profil = pd.DataFrame(item)
            elif item['name'] == 'Visites sur la Page':
                visites_sur_la_page = pd.DataFrame(item)
            elif item['name'] == 'Events visited':
                events_visited = pd.DataFrame(item)
            elif item['name'] == 'Groups visited':
                groups_visited = pd.DataFrame(item)
            else:
                pass

    if visites_de_profil is not None:
        visites_de_profil = transform1(visites_de_profil)
    if visites_sur_la_page is not None:
        visites_sur_la_page = transform1(visites_sur_la_page)
    if events_visited is not None:
        events_visited = transform1(events_visited)
    if groups_visited is not None:
        groups_visited = transform1(groups_visited)

    try:
        apps_and_websites = pd.read_json(folder_name + '/apps_and_websites/apps_and_websites.json')
    except:
        print('apps_and_websites/apps_and_websites.json does not exist', file=sys.stderr)
        apps_and_websites = None

    if apps_and_websites is not None and not apps_and_websites.empty:
        apps_and_websites['timestamp'] = apps_and_websites.apply(
            lambda x: datetime.fromtimestamp(x['installed_apps']['added_timestamp']), axis=1)
        apps_and_websites['name'] = apps_and_websites.apply(
            lambda x: x['installed_apps']['name'].encode('latin1').decode('utf8'), axis=1)
        apps_and_websites = apps_and_websites.drop(columns=['installed_apps'])

    try:
        event_invitations = pd.read_json(folder_name + '/events/event_invitations.json')
    except:
        print('events/event_invitations.json does not exist', file=sys.stderr)
        event_invitations = None

    if event_invitations is not None and not event_invitations.empty:
        event_invitations['start_timestamp'] = event_invitations.apply(
            lambda x: datetime.fromtimestamp(x['events_invited']['start_timestamp']), axis=1)
        event_invitations['end_timestamp'] = event_invitations.apply(
            lambda x: datetime.fromtimestamp(x['events_invited']['end_timestamp']), axis=1)
        event_invitations['name'] = event_invitations.apply(
            lambda x: x['events_invited']['name'].encode('latin1').decode('utf8'), axis=1)
        event_invitations = event_invitations.drop(columns=['events_invited'])
        event_invitations['timestamp'] = event_invitations['start_timestamp']

    try:
        your_event_responses = pd.read_json(folder_name + '/events/your_event_responses.json')
    except:
        print('events/your_event_responses.json does not exist', file=sys.stderr)
        your_event_responses = None

    if your_event_responses is not None and not your_event_responses.empty and 'events_declined' in \
            your_event_responses.index:
        events_declined = pd.DataFrame(your_event_responses['event_responses']['events_declined'])
    else:
        events_declined = None
    if your_event_responses is not None and not your_event_responses.empty and 'events_interested' in \
            your_event_responses.index:
        events_interested = pd.DataFrame(your_event_responses['event_responses']['events_interested'])
    else:
        events_interested = None
    if your_event_responses is not None and not your_event_responses.empty and 'events_joined' in \
            your_event_responses.index:
        events_joined = pd.DataFrame(your_event_responses['event_responses']['events_joined'])
    else:
        events_joined = None

    if events_declined is not None:
        events_declined = transform2(events_declined)
    if events_interested is not None:
        events_interested = transform2(events_interested)
    if events_joined is not None:
        events_joined = transform2(events_joined)

    try:
        followed_pages = pd.read_json(folder_name + '/following_and_followers/followed_pages.json')
    except:
        print('following_and_followers/followed_pages.json does not exist', file=sys.stderr)
        followed_pages = None

    if followed_pages is not None and not followed_pages.empty:
        followed_pages['timestamp'] = followed_pages['pages_followed'].apply(
            lambda x: datetime.fromtimestamp(x['timestamp']))
        followed_pages['name'] = followed_pages['pages_followed'].apply(
            lambda x: x['title'].encode('latin1').decode('utf8'))
        followed_pages = followed_pages.drop(columns=['pages_followed'])

    try:
        following = pd.read_json(folder_name + '/following_and_followers/following.json')
    except:
        print('following_and_followers/following.json does not exist', file=sys.stderr)
        following = None

    if following is not None and not following.empty:
        following['timestamp'] = following['following'].apply(lambda x: datetime.fromtimestamp(x['timestamp']))
        following['name'] = following['following'].apply(lambda x: x['name'].encode('latin1').decode('utf8'))
        following = following.drop(columns=['following'])

    try:
        unfollowed_pages = pd.read_json(folder_name + '/following_and_followers/unfollowed_pages.json')
    except:
        print('following_and_followers/unfollowed_pages.json does not exist', file=sys.stderr)
        unfollowed_pages = None

    if unfollowed_pages is not None and not unfollowed_pages.empty:
        unfollowed_pages['timestamp'] = unfollowed_pages['pages_unfollowed'].apply(
            lambda x: datetime.fromtimestamp(x['timestamp']))
        unfollowed_pages['name'] = unfollowed_pages['pages_unfollowed'].apply(
            lambda x: x['title'].encode('latin1').decode('utf8'))
        unfollowed_pages = unfollowed_pages.drop(columns=['pages_unfollowed'])

    try:
        friends = pd.read_json(folder_name + '/friends/friends.json')
    except:
        print('friends/friends.json does not exist', file=sys.stderr)
        friends = None

    if friends is not None and not friends.empty:
        friends['timestamp'] = friends['friends'].apply(lambda x: datetime.fromtimestamp(x['timestamp']))
        friends['name'] = friends['friends'].apply(lambda x: x['name'].encode('latin1').decode('utf8'))
        friends = friends.drop(columns=['friends'])

    try:
        received_friend_requests = pd.read_json(folder_name + '/friends/received_friend_requests.json')
    except:
        print('friends/received_friend_requests.json does not exist', file=sys.stderr)
        received_friend_requests = None

    try:
        rejected_friend_requests = pd.read_json(folder_name + '/friends/rejected_friend_requests.json')
    except:
        print('friends/rejected_friend_requests.json does not exist', file=sys.stderr)
        rejected_friend_requests = None

    try:
        sent_friend_requests = pd.read_json(folder_name + '/friends/sent_friend_requests.json')
    except:
        print('friends/sent_friend_requests.json does not exist', file=sys.stderr)
        sent_friend_requests = None

    try:
        removed_friends = pd.read_json(folder_name + '/friends/removed_friends.json')
    except:
        print('friends/removed_friends.json does not exist', file=sys.stderr)
        removed_friends = None

    if received_friend_requests is not None and not received_friend_requests.empty:
        received_friend_requests = transform(received_friend_requests, 'received_requests')
    if rejected_friend_requests is not None and not rejected_friend_requests.empty:
        rejected_friend_requests = transform(rejected_friend_requests, 'rejected_requests')
    if sent_friend_requests is not None and not sent_friend_requests.empty:
        sent_friend_requests = transform(sent_friend_requests, 'sent_requests')
    if removed_friends is not None and not removed_friends.empty:
        removed_friends = transform(removed_friends, 'deleted_friends')

    try:
        groups_joined = pd.read_json(folder_name + '/groups/your_group_membership_activity.json')
    except:
        print('groups/your_group_membership_activity.json does not exist', file=sys.stderr)
        groups_joined = None

    if groups_joined is not None and not groups_joined.empty:
        groups_joined['timestamp'] = groups_joined['groups_joined'].apply(
            lambda x: datetime.fromtimestamp(x['timestamp']))
        groups_joined['name'] = groups_joined['groups_joined'].apply(
            lambda x: x['title'].encode('latin1').decode('utf8'))
        groups_joined = groups_joined.drop(columns=['groups_joined'])

#enlevé car dépendant de la langue du profil. C'est un prospect
    # if groups_joined is not None and ~groups_joined.empty:
    #     groups_joined['name'] = groups_joined['name'].apply(lambda x: x.split('Vous '))
    #     groups_joined['name'] = groups_joined['name'].apply(lambda x: x[1].split('membre de '))
    #     groups_joined['action'] = groups_joined['name'].apply(lambda x: x[0])
    #     groups_joined['name'] = groups_joined['name'].apply(lambda x: x[1][0:len(x[1]) - 1])
    #
    # if groups_joined is not None and ~groups_joined.empty:
    #     groups_joined['action'] = groups_joined['action'].apply(
    #         lambda x: 'joined' if 'devenu' in x else 'quit' if 'arrêté' in x else 'autre')

    try:
        your_groups = pd.read_json(folder_name + '/groups/your_groups.json')
    except:
        print('groups/your_groups.json does not exist', file=sys.stderr)
        your_groups = None

    if your_groups is not None and not your_groups.empty:
        your_groups['timestamp'] = your_groups['groups_admined'].apply(lambda x: datetime.fromtimestamp(x['timestamp']))
        your_groups['name'] = your_groups['groups_admined'].apply(lambda x: x['name'].encode('latin1').decode('utf8'))
        your_groups = your_groups.drop(columns=['groups_admined'])

    try:
        people = pd.read_json(folder_name + '/interactions/people.json')
    except:
        print('interactions/people.json does not exist', file=sys.stderr)
        people = None

    if people is not None and not people.empty:
        people = pd.DataFrame(people['people_interactions'][0]['entries'])
        people['timestamp'] = people['timestamp'].apply(lambda x: datetime.fromtimestamp(x))
        people['name'] = people['data'].apply(lambda x: x['name'].encode('latin1').decode('utf8'))
        people['URL'] = people['data'].apply(lambda x: x['uri'])
        people = people.drop(columns=['data'])

    # filenames est une liste de tous les dossiers dans 'messages/inbox'.
    try:
        filenames = os.listdir(folder_name + '/messages/inbox')
    except:
        print('messages/inbox does not exist', file=sys.stderr)
        filenames = []
    # On ajoute au début "messages/inbox" pour entrer dans le bon directory
    # et à la fin "message_1.json" car c'est le fichier JSON d'intérêt
    filenames = [folder_name + '/messages/inbox/' + filename + '/message_1.json' for filename in filenames]

    inbox = pd.DataFrame(columns=['content', 'type', 'participants', 'timestamp_ms', 'sender_name'])

    for i in range(0, len(filenames)):
        with open(filenames[i], 'r') as f:
            r = json.load(f)
        temp = pd.DataFrame(r['messages'])
        names = [x['name'] for x in r['participants']]
        temp['participants'] = '; '.join(name.encode('latin1').decode('utf8') for name in names)
        temp['timestamp_ms'] = temp['timestamp_ms'].apply(lambda x: datetime.fromtimestamp(round(x / 1000)))
        if 'content' in temp.columns:
            temp['content'] = temp['content'].apply(lambda x: str(x).encode('latin1').decode('utf8'))
        temp['sender_name'] = temp['sender_name'].apply(lambda x: x.encode('latin1').decode('utf8'))
        # temp = temp[['content', 'type', 'participant', 'timestamp', 'sender']]
        inbox = pd.concat([inbox, temp], sort=True)
    inbox['participants'] = inbox['participants'].apply(lambda x: x.split('; '))

    inbox.rename(columns={'timestamp_ms': 'timestamp', 'participants': 'name'}, inplace=True)
    inbox.index = range(0, len(inbox.index))

    try:
        filenames = os.listdir(folder_name + '/messages/message_requests')
    except:
        print('messages/message_requests does not exist', file=sys.stderr)
        filenames = []
    # On ajoute au début "messages/inbox" pour entrer dans le bon directory
    # et à la fin "message_1.json" car c'est le fichier JSON d'intérêt
    filenames = [folder_name + '/messages/message_requests/' + filename + '/message_1.json' for filename in filenames]

    msg_requests = pd.DataFrame(columns=['content', 'type', 'participants', 'timestamp_ms', 'sender_name'])

    for i in range(0, len(filenames)):
        with open(filenames[i], 'r') as f:
            r = json.load(f)
        temp = pd.DataFrame(r['messages'])
        names = [x['name'] for x in r['participants']]
        temp['participants'] = '; '.join(name.encode('latin1').decode('utf8') for name in names)
        temp['timestamp_ms'] = temp['timestamp_ms'].apply(lambda x: datetime.fromtimestamp(round(x / 1000)))
        temp['content'] = temp['content'].apply(lambda x: str(x).encode('latin1').decode('utf8'))
        temp['sender_name'] = temp['sender_name'].apply(lambda x: x.encode('latin1').decode('utf8'))
        # temp = temp[['content', 'type', 'participant', 'timestamp', 'sender']]
        msg_requests = pd.concat([msg_requests, temp], sort=True)
    msg_requests['participants'] = msg_requests['participants'].apply(lambda x: x.split('; '))

    msg_requests.rename(columns={'timestamp_ms': 'timestamp', 'participants': 'name'}, inplace=True)
    msg_requests.index = range(0, len(msg_requests.index))

    try:
        with open(folder_name + '/other_activity/pokes.json', 'r') as f:
            r = json.load(f)
    except:
        print('other_activity/pokes.json does not exist', file=sys.stderr)
        r = None

    if r is not None and 'pokes' in r.keys() and 'activity_log_data' in r['pokes'].keys():
        pokes = pd.DataFrame(r['pokes']['activity_log_data'])
    else:
        pokes = None

    if pokes is not None and not pokes.empty:
        pokes['timestamp'] = pokes['timestamp'].apply(lambda x: datetime.fromtimestamp(x))
        pokes['data'] = pokes['data'].apply(lambda x: x[0]['name'].encode('latin1').decode('utf8'))
        pokes = pokes.drop(columns=['title'])
        pokes.rename(columns={'data': 'name'}, inplace=True)

    try:
        support = pd.read_json(folder_name + '/other_activity/support_correspondences.json')
    except:
        print('other_activity/support_correspondences.json does not exist', file=sys.stderr)
        support = None

    if support is not None and not support.empty:
        support['timestamp'] = support['support_correspondence'].apply(lambda x: datetime.fromtimestamp(x['timestamp']))
        support['subject'] = support['support_correspondence'].apply(
            lambda x: x['subject'].encode('latin1').decode('utf8'))
        support['support_correspondence'] = support['support_correspondence'].apply(lambda x: x['messages'])
        support.rename(columns={'support_correspondence': 'messages', 'subject': 'name'}, inplace=True)

    try:
        your_pages = pd.read_json(folder_name + '/pages/your_pages.json')
    except:
        print('pages/your_pages.json does not exist', file=sys.stderr)
        your_pages = None

    if your_pages is not None and not your_pages.empty:
        your_pages['name'] = your_pages['pages'].apply(lambda x: x['name'].encode('latin1').decode('utf8'))
        your_pages['timestamp'] = your_pages['pages'].apply(lambda x: datetime.fromtimestamp(x['timestamp']))
        your_pages['URL'] = your_pages['pages'].apply(lambda x: x['url'])
        your_pages = your_pages.drop(columns=['pages'])

    try:
        search_history = pd.read_json(folder_name + '/search_history/your_search_history.json')
    except:
        print('search_history/your_search_history.json does not exist', file=sys.stderr)
        search_history = None

    if search_history is not None and not search_history.empty:
        search_history['typed'] = search_history['searches'].apply(
            lambda x: x['data'][0]['text'].encode('latin1').decode('utf8') if 'data' in x.keys() else None)
        search_history['clicked'] = search_history['searches'].apply(
            lambda x: x['attachments'][0]['data'][0]['text'].encode('latin1').decode('utf8'))
        search_history['timestamp'] = search_history['searches'].apply(lambda x: datetime.fromtimestamp(x['timestamp']))
        search_history = search_history.drop(columns=['searches'])
        search_history.rename(columns={'clicked': 'name'}, inplace=True)

    try:
        comments = pd.read_json(folder_name + '/comments/comments.json')
    except:
        print('comments/comments.json does not exist', file=sys.stderr)
        comments = None

    if comments is not None and not comments.empty:
        comments['timestamp'] = comments['comments'].apply(lambda x: datetime.fromtimestamp(x['timestamp']))
        comments['content'] = comments['comments'].apply(lambda x: x['title'].encode('latin1').decode('utf8'))
        comments['data'] = comments['comments'].apply(
            lambda x: x['data'][0]['comment']['comment'].encode('latin1').decode(
                'utf8') if 'data' in x.keys() else None)
        comments['content'] = comments['content'] + comments['data']
        comments = comments.drop(columns=['comments', 'data'])
        comments.rename(columns={'content': 'name'}, inplace=True)

    try:
        liked_posts_and_comments = pd.read_json(folder_name + '/likes_and_reactions/posts_and_comments.json')
    except:
        print('likes_and_reactions/posts_and_comments.json does not exist', file=sys.stderr)
        liked_posts_and_comments = None

    if liked_posts_and_comments is not None and not liked_posts_and_comments.empty:
        liked_posts_and_comments['timestamp'] = liked_posts_and_comments['reactions'].apply(
            lambda x: datetime.fromtimestamp(x['timestamp']))
        liked_posts_and_comments['content'] = liked_posts_and_comments['reactions'].apply(
            lambda x: x['title'].encode('latin1').decode('utf8'))
        liked_posts_and_comments['data'] = liked_posts_and_comments['reactions'].apply(
            lambda x: x['data'][0]['reaction']['reaction'])
        liked_posts_and_comments['content'] = liked_posts_and_comments['content'] + liked_posts_and_comments['data']
        liked_posts_and_comments = liked_posts_and_comments.drop(columns=['reactions', 'data'])
        liked_posts_and_comments.rename(columns={'content': 'name'}, inplace=True)

    try:
        liked_pages = pd.read_json(folder_name + '/likes_and_reactions/pages.json')
    except:
        print('likes_and_reactions/pages.json does not exist', file=sys.stderr)
        liked_pages = None

    if liked_pages is not None and not liked_pages.empty:
        liked_pages['timestamp'] = liked_pages['page_likes'].apply(lambda x: datetime.fromtimestamp(x['timestamp']))
        liked_pages['name'] = liked_pages['page_likes'].apply(lambda x: x['name'].encode('latin1').decode('utf8'))
        liked_pages = liked_pages.drop(columns=['page_likes'])

    try:
        filenames = os.listdir(folder_name + '/messages/archived_threads')
    except:
        print('messages/archived_threads does not exist', file=sys.stderr)
        filenames = []
    # On ajoute au début "messages/inbox" pour entrer dans le bon directory
    # et à la fin "message_1.json" car c'est le fichier JSON d'intérêt
    filenames = [folder_name + '/messages/archived_threads/' + filename + '/message_1.json' for filename in filenames]

    archived_threads = pd.DataFrame(columns=['content', 'type', 'participants', 'timestamp_ms', 'sender_name'])

    for i in range(0, len(filenames)):
        with open(filenames[i], 'r') as f:
            r = json.load(f)
        temp = pd.DataFrame(r['messages'])
        names = [x['name'] for x in r['participants']]
        temp['participants'] = '; '.join(name.encode('latin1').decode('utf8') for name in names)
        temp['timestamp_ms'] = temp['timestamp_ms'].apply(lambda x: datetime.fromtimestamp(round(x / 1000)))
        if 'content' in temp.columns:
            temp['content'] = temp['content'].apply(lambda x: str(x).encode('latin1').decode('utf8'))
        temp['sender_name'] = temp['sender_name'].apply(lambda x: x.encode('latin1').decode('utf8'))
        # temp = temp[['content', 'type', 'participant', 'timestamp', 'sender']]
        archived_threads = pd.concat([archived_threads, temp], sort=True)
    archived_threads['participants'] = archived_threads['participants'].apply(lambda x: x.split('; '))

    archived_threads.rename(columns={'timestamp_ms': 'timestamp', 'participants': 'name'}, inplace=True)
    archived_threads.index = range(0, len(archived_threads.index))

    try:
        filenames = os.listdir(folder_name + '/messages/filtered_threads')
    except:
        print('messages/filtered_threads does not exist', file=sys.stderr)
        filenames = []
    # On ajoute au début "messages/inbox" pour entrer dans le bon directory
    # et à la fin "message_1.json" car c'est le fichier JSON d'intérêt
    filenames = [folder_name + '/messages/filtered_threads/' + filename + '/message_1.json' for filename in filenames]

    filtered_threads = pd.DataFrame(columns=['content', 'type', 'participants', 'timestamp_ms', 'sender_name'])

    for i in range(0, len(filenames)):
        with open(filenames[i], 'r') as f:
            r = json.load(f)
        temp = pd.DataFrame(r['messages'])
        names = [x['name'] for x in r['participants']]
        temp['participants'] = '; '.join(name.encode('latin1').decode('utf8') for name in names)
        temp['timestamp_ms'] = temp['timestamp_ms'].apply(lambda x: datetime.fromtimestamp(round(x / 1000)))
        if 'content' in temp.columns:
            temp['content'] = temp['content'].apply(lambda x: str(x).encode('latin1').decode('utf8'))
        temp['sender_name'] = temp['sender_name'].apply(lambda x: x.encode('latin1').decode('utf8'))
        # temp = temp[['content', 'type', 'participant', 'timestamp', 'sender']]
        filtered_threads = pd.concat([filtered_threads, temp], sort=True)
    filtered_threads['participants'] = filtered_threads['participants'].apply(lambda x: x.split('; '))

    filtered_threads.rename(columns={'timestamp_ms': 'timestamp', 'participants': 'name'}, inplace=True)
    filtered_threads.index = range(0, len(filtered_threads.index))

    try:
        with open(folder_name + '/groups/your_posts_and_comments_in_groups.json', 'r') as f:
            r = json.load(f)
    except:
        print('groups/your_posts_and_comments_in_groups.json does not exist', file=sys.stderr)
        r = None

    if r is not None and 'group_posts' in r.keys() and 'activity_log_data' in r['group_posts'].keys():
        comments_in_groups = pd.DataFrame(r['group_posts']['activity_log_data'])
    else:
        comments_in_groups = None

    if comments_in_groups is not None and not comments_in_groups.empty:
        comments_in_groups['timestamp'] = comments_in_groups['timestamp'].apply(lambda x: datetime.fromtimestamp(x))
        comments_in_groups['title'] = comments_in_groups['title'].apply(lambda x: x.encode('latin1').decode('utf8'))
        comments_in_groups['comment'] = comments_in_groups['data'].apply(
            lambda x: x[0]['comment']['comment'].encode('latin1').decode('utf8') if type(
                x) == list and x != [] and 'comment' in x[0].keys() else None)
        comments_in_groups['group'] = comments_in_groups['data'].apply(
            lambda x: x[0]['comment']['group'].encode('latin1').decode('utf8') if type(
                x) == list and x != [] and 'comment' in x[0].keys() and 'group' in x[0]['comment'].keys() else None)
        comments_in_groups['attachments'] = comments_in_groups['attachments'].apply(
            lambda x: x[0]['data'][0] if type(x) == list and x != [] else None)
        comments_in_groups = comments_in_groups.drop(columns=['data'])
        comments_in_groups.rename(columns={'title': 'name'}, inplace=True)

#toutes les df pertinentes ont été créées
    created_df = {'friends': friends, 'visites_de_profil': visites_de_profil,
                  'visites_sur_la_page': visites_sur_la_page,
                  'events_visited': events_visited, 'groups_visited': groups_visited,
                  'event_invitations': event_invitations,
                  'events_interested': events_interested, 'events_joined': events_joined,
                  'events_declined': events_declined,
                  'followed_pages': followed_pages, 'following': following, 'unfollowed_pages': unfollowed_pages,
                  'received_friend_requests': received_friend_requests,
                  'rejected_friend_requests': rejected_friend_requests,
                  'sent_friend_requests': sent_friend_requests, 'removed_friends': removed_friends,
                  'groups_joined': groups_joined, 'pokes': pokes, 'support': support, 'your_pages': your_pages,
                  'search_history': search_history, 'comments': comments,
                  'liked_posts_and_comments': liked_posts_and_comments,
                  'liked_pages': liked_pages, 'archived_threads': archived_threads,
                  'filtered_threads': filtered_threads,
                  'comments_in_groups': comments_in_groups, 'people': people, 'inbox': inbox,
                  'msg_requests': msg_requests, 'voir_en_premier': voir_en_premier, 'voir_moins': voir_moins,
                  'apps_and_websites': apps_and_websites}


#on ne garde que le nom et la date pour plot l'activité mensuelle
    temp = pd.DataFrame(columns=['name', 'timestamp'])
    for key in created_df.keys():
        if isinstance(created_df[key], pd.DataFrame) and not created_df[key].empty:
            temp = pd.concat([temp, created_df[key][['name', 'timestamp']]])

    temp.set_index('timestamp', inplace = True)
    if not temp.empty:
        temp = temp['name'].resample('MS').size()
    temp.sort_index(inplace = True)
    monthly_count = pd.DataFrame(index=temp.index, columns=created_df.keys())

    for key in created_df.keys():
        if isinstance(created_df[key], pd.DataFrame) and not created_df[key].empty:
            temp = created_df[key].set_index('timestamp')
            temp = temp['name'].resample('MS').size()
            temp.sort_index(inplace=True)
            monthly_count[key] = temp

    monthly_count.dropna(axis=1, how='all', inplace=True)
    monthly_count.fillna(int(0), inplace=True)

#plot uniquement ceux avec std entre 1 et 10 (pour que la comparaison visuelle soit agréable
    viz = monthly_count.drop(columns=monthly_count.columns[(monthly_count.std() < 1)], axis=1)
    viz = viz.drop(columns=monthly_count.columns[(monthly_count.std() > 10)], axis=1)

    try:
        os.mkdir(folder_name + '_results')
    except FileExistsError as e:
        print('Warning, the folder was already existing and some data may have been lost', file=sys.stderr)

    layout = go.Layout(template="plotly_white")

    fig = go.Figure(layout=layout)


    for col in viz.columns:
        col = str(col)
        temp = viz[col].sort_values()
        fig.add_trace(go.Bar(x=temp.index, y=temp, name=col))

    fig.update_layout(barmode='stack', xaxis_tickangle=-45)  # ou group

    fig.update_layout(title_text='Monthly new occurrences count of various Facebook actions <br> when 1<standard deviation<10')

    fig.show()

    plotly.offline.plot(fig, filename= folder_name + '_results/month_count_actions_1-std-10.html', auto_open=False)

#plot de ceux avec std plus grand que 10
    viz = monthly_count.drop(columns=monthly_count.columns[(monthly_count.std() < 10)], axis=1)

    layout = go.Layout(template="plotly_white")

    fig = go.Figure(layout=layout)

    for col in viz.columns:
        col = str(col)
        temp = viz[col].sort_values()

        fig.add_trace(go.Bar(x=temp.index, y=temp, name=col))

    fig.update_layout(barmode='stack', xaxis_tickangle=-45)  # ou group

    fig.update_layout(
        title_text='Monthly new occurrences count of various Facebook actions <br> when 10<standard deviation')

    fig.show()

    plotly.offline.plot(fig, filename=folder_name + '_results/month_count_actions_10-std.html', auto_open=False)

#séparer les conversations entre deux personnes et de groupe
    inbox['flag'] = inbox['name'].apply(lambda x: len(x) > 1)
    inbox_clean = inbox[inbox['flag']]

    inbox_clean['group'] = inbox_clean['name'].apply(lambda x: aggregate_liste(x)[1])
    inbox_clean['name'] = inbox_clean['name'].apply(lambda x: aggregate_liste(x)[0])

    conv = inbox_clean[inbox_clean['group'] == False]
    groups = inbox_clean[inbox_clean['group'] == True]

    names_conv = conv['name'].unique()
    names_groups = groups['name'].unique()

    groups_dict = {}
    for name in names_groups:
        groups_dict[name] = groups[groups['name'] == name][['name', 'timestamp']]

    names_dict = {}
    for name in names_conv:
        names_dict[name] = conv[conv['name'] == name][['name', 'timestamp']]

    inbox_viz_conv = conv.set_index('timestamp')
    if not inbox_viz_conv.empty :
        inbox_viz_conv = inbox_viz_conv['name'].resample('MS').size()

    inbox_viz_groups = groups.set_index('timestamp')
    if not inbox_viz_groups.empty:
        inbox_viz_groups = inbox_viz_groups['name'].resample('MS').size()

    monthly_inbox_conv = pd.DataFrame(index=inbox_viz_conv.index, columns=names_dict.keys())
    monthly_inbox_groups = pd.DataFrame(index=inbox_viz_groups.index, columns=groups_dict.keys())

    for key in names_dict.keys():
        if isinstance(names_dict[key], pd.DataFrame) and not names_dict[key].empty:
            temp = names_dict[key].set_index('timestamp')
            temp = temp['name'].resample('MS').size()
            temp.sort_index(inplace=True)
            monthly_inbox_conv[key] = temp

    for key in groups_dict.keys():
        if isinstance(groups_dict[key], pd.DataFrame) and not groups_dict[key].empty:
            temp = groups_dict[key].set_index('timestamp')
            temp = temp['name'].resample('MS').size()
            temp.sort_index(inplace=True)
            monthly_inbox_groups[key] = temp

#garde seulement ceux qui ont un total de messages dans le top 10 ou un pic mensuel dans le top 10
    viz1 = monthly_inbox_conv.drop(columns=monthly_inbox_conv.columns[monthly_inbox_conv.count() <=
                                                                      monthly_inbox_conv.count().sort_values(
                                                                          ascending=False).head(10)[-1]], axis=1)

    viz2 = monthly_inbox_conv.drop(columns=monthly_inbox_conv.columns[monthly_inbox_conv.max() <=
                                                                      monthly_inbox_conv.max().sort_values(
                                                                          ascending=False).head(10)[-1]], axis=1)

    layout = go.Layout(template="plotly_white")

    fig = go.Figure(layout=layout)

    for col in viz1.columns:
        col = str(col)
        temp = viz1[col].sort_values()
        fig.add_trace(go.Bar(x=temp.index, y=temp, name=col))

    for col in viz2.columns:
        col = str(col)
        temp = viz2[col].sort_values()
        fig.add_trace(go.Bar(x=temp.index, y=temp, name=col))

    fig.update_layout(barmode='stack', xaxis_tickangle=-45)  # ou group

    fig.update_layout(
        title_text='Monthly exchanged messages count in private conversations <br> for the 10 conversations with the '
                   'highest total count <br> and for the 10 conversations with the highest month count')

    fig.show()

    plotly.offline.plot(fig, filename=folder_name + '_results/inbox_conv.html', auto_open=False)

    fig = go.Figure(layout=layout)

    legend = 0

    for col in viz1.columns:
        col = str(col)
        temp = viz1[col].sort_values()
        fig.add_trace(go.Bar(x=temp.index, y=temp, name=legend))
        legend += 1

    for col in viz2.columns:
        col = str(col)
        temp = viz2[col].sort_values()
        fig.add_trace(go.Bar(x=temp.index, y=temp, name=legend))
        legend += 1

    fig.update_layout(barmode='stack', xaxis_tickangle=-45)  # ou group

    fig.update_layout(title_text='Anonymized monthly exchanged messages count in private conversations <br> for the 10 '
                                 'conversations with the highest total count <br> and for the 10 conversations with the highest month count')

    fig.show()

    plotly.offline.plot(fig, filename=folder_name + '_results/inbox_conv_anonymized.html', auto_open=True)

    viz1 = monthly_inbox_groups.drop(columns=monthly_inbox_groups.columns[monthly_inbox_groups.count() <=
                                                                          monthly_inbox_groups.count().sort_values(
                                                                              ascending=False).head(10)[-1]], axis=1)
    viz2 = monthly_inbox_groups.drop(columns=monthly_inbox_groups.columns[monthly_inbox_groups.max() <=
                                                                          monthly_inbox_groups.max().sort_values(
                                                                              ascending=False).head(10)[-1]], axis=1)


    fig = go.Figure(layout=layout)

    for col in viz1.columns:
        col = str(col)
        temp = viz1[col].sort_values()
        n = math.floor(len(col) / 60)
        legend = ''
        for i in range(0, n):
            legend = legend + col[i * 60: (i + 1) * 60] + '<br>'
        legend += col[n * 60:]
        fig.add_trace(go.Bar(x=temp.index, y=temp, name=legend))

    for col in viz2.columns:
        col = str(col)
        temp = viz2[col].sort_values()
        n = math.floor(len(col) / 60)
        legend = ''
        for i in range(0, n):
            legend = legend + col[i * 60: (i + 1) * 60] + '<br>'
        legend += col[n * 60:]
        fig.add_trace(go.Bar(x=temp.index, y=temp, name=legend))

    fig.update_layout(barmode='stack', xaxis_tickangle=-45)  # ou group

    fig.update_layout(
        title_text='Monthly exchanged messages count in group conversations <br> for the 10 conversations with the '
                   'highest total count <br> and for the 10 conversations with the highest month count')

    fig.show()

    plotly.offline.plot(fig, filename=folder_name + '_results/inbox_group.html', auto_open=False)

    fig = go.Figure(layout=layout)

    legend = 0

    for col in viz1.columns:
        col = str(col)
        temp = viz1[col].sort_values()
        fig.add_trace(go.Bar(x=temp.index, y=temp, name=legend))
        legend += 1

    for col in viz2.columns:
        col = str(col)
        temp = viz2[col].sort_values()
        fig.add_trace(go.Bar(x=temp.index, y=temp, name=legend))
        legend += 1

    fig.update_layout(barmode='stack', xaxis_tickangle=-45)  # ou group

    fig.update_layout(title_text='Anonymized monthly exchanged messages count in group conversations <br> for the 10 '
                                 'conversations with the highest total count <br> and for the 10 conversations with the highest month count')

    fig.show()

    plotly.offline.plot(fig, filename=folder_name + '_results/inbox_group_anonymized.html', auto_open=False)

    try:
        os.mkdir(folder_name + '_results')
    except FileExistsError as e:
        print('Warning, the folder was already existing and some data may have been lost', file=sys.stderr)

    for key in created_df.keys():
        if isinstance(created_df[key], pd.DataFrame) and not created_df[key].empty:
            created_df[key].to_csv(path_or_buf = folder_name + '_results/' + key + '.csv')