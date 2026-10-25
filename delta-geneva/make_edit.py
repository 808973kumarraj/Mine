"""Write edit.json, the Geneva document for "How Delta Lake works".

usage: python3 make_edit.py && geneva render edit.json -o delta-lake.mp4
One HTML clip per scene (scenes/*.html, timed from voice/scenes.json), the FilmCraft mix
(audio/mix.m4a) as the soundtrack, subtitles burned in from voice/narration.srt and also muxed as a track.
"""
import json, pathlib

HERE = pathlib.Path(__file__).parent
meta = json.load(open(HERE / 'voice/scenes.json'))
doc = {
    'geneva': '1.1',
    'output': {'width': 1920, 'height': 1080, 'fps': 30},
    'assets': {'mix': {'src': 'audio/mix.m4a'}, 'subs': {'src': 'voice/narration.srt', 'kind': 'captions'},
               'subtrack': {'src': 'voice/narration.srt', 'kind': 'subtitle'},
               **{s['name']: {'src': f"scenes/{s['name']}.html"} for s in meta['scenes']}},
    'layers': [
        {'id': 'scenes', 'clips': [{'source': {'kind': 'html', 'asset': s['name']}, 'start': f"{s['start']}s", 'duration': f"{s['duration']}s"}
                                   for s in meta['scenes']]},
        {'id': 'captions', 'clips': [{'source': {
            'kind': 'captions', 'asset': 'subs', 'margin': '5%',
            'style': {'font': '600 38px/1.35 JetBrains Mono', 'color': '#ffffff', 'background': '#1e1b17d9',
                      'padding': '14px', 'radius': '10px', 'max_width': '78%'}}}]},
        {'id': 'soundtrack', 'clips': [{'source': {'kind': 'audio', 'asset': 'mix'}}]},
    ],
    'subtitles': [{'asset': 'subtrack', 'language': 'en', 'title': 'English'}],
}
(HERE / 'edit.json').write_text(json.dumps(doc, indent=1))
print('edit.json:', len(meta['scenes']), 'scenes,', meta['duration'], 's')
