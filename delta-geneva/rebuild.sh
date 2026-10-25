#!/bin/bash
# Rebuild everything that depends on the narration, in order:
# voice (Chatterbox) -> scene timings -> SoundCraft score -> FilmCraft mix -> scenes -> HTML preview.
# usage: APPS=<dir with the ArtCraft apps> PY=<python with chatterbox-tts> FONTS=<fonts.css> bash rebuild.sh
set -e
cd "$(dirname "$0")"
$PY voice/script.py voice
cd audio && python3 score.py score.mid
S=$(find $APPS/soundcraft -name soundcraft-cli -type f); F=$(find $APPS/filmcraft -name filmcraft-cli -type f)
D=$(python3 -c "import json;print(json.load(open('../voice/scenes.json'))['duration'])")
rm -f score.scraft mix.fcproj
$S run --cmd "file.import_midi={\"path\":\"$PWD/score.mid\"}" --save $PWD/score.scraft >/dev/null
$S run --in score.scraft --bounce $PWD/score.wav --start 0 --end $D >/dev/null
printf '%s\n' "{\"id\":\"file.import\",\"params\":{\"paths\":[\"$(realpath ../voice/narration.wav)\",\"$PWD/score.wav\"]}}" \
  '{"id":"file.newSequence","params":{"name":"Mix","fps":30,"video":1,"audio":2}}' \
  '{"id":"timeline.place","params":{"item":1,"track":"A1","seconds":0,"video":false}}' \
  '{"id":"timeline.place","params":{"item":2,"track":"A2","seconds":0,"video":false}}' \
  '{"id":"essentialSound.setType","params":{"clips":[7],"type":"dialogue"}}' \
  '{"id":"essentialSound.setType","params":{"clips":[8],"type":"music"}}' \
  '{"id":"essentialSound.set","params":{"clips":[8],"values":{"ducking.reduceDb":-12}}}' \
  '{"id":"essentialSound.generateDucking","params":{"clips":[8]}}' \
  "{\"id\":\"file.saveAs\",\"params\":{\"path\":\"$PWD/mix.fcproj\"}}" \
  "{\"id\":\"file.exportMedia\",\"params\":{\"path\":\"$PWD/mix.m4a\",\"format\":\"aac\",\"bitrateKbps\":128,\"loudnessLufs\":-16,\"range\":\"custom\",\"startSeconds\":0,\"endSeconds\":$D,\"wait\":true}}" \
  | $F run - | grep -c '"ok":true'
rm -rf "FilmCraft Previews" score.wav
cd ..
python3 make_scenes.py >/dev/null
python3 make_edit.py
python3 make_preview.py ../delta-lake-preview.html
echo REBUILD DONE
