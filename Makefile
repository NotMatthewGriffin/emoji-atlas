.phony: emoji-atlas
emoji-atlas:
	docker build -t emoji_atlas -f docker/Dockerfile src/

.phony: emoji-fastlas
emoji-fastlas:
	docker build -t emoji_atlas -f docker/Dockerfile-pyston src/
