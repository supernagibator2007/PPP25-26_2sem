import datetime
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

import schemas
from models import get_db, Post


router = APIRouter(prefix='/posts')


@router.get('/', response_model=list[schemas.PostResponse])
def get_posts(author=None, db: Session = Depends(get_db)):
    query = db.query(Post)
    if author:
        query = query.filter(Post.author == author)
    return query.all()

@router.get("/{id}", response_model=schemas.PostResponse)
def get_post_by_id(id: int, db: Session = Depends(get_db)):
    post = db.query(Post).filter(Post.id == id).first()
    if not post:
        raise HTTPException(status_code=404, detail="Post not found")
    return post

@router.post("/", response_model=schemas.PostResponse, status_code=status.HTTP_201_CREATED)
def create_post(post: schemas.PostBase, db: Session = Depends(get_db)):
    extracted_str = datetime.datetime.now().strftime('%Y/%m/%d %H:%M:%S')
    new_post = Post(**post.model_dump(), extracted_time=extracted_str)
    db.add(new_post)
    db.commit()
    db.refresh(new_post)
    return new_post

@router.put("/{id}", response_model=schemas.PostResponse)
def update_post_put(id, updated: schemas.PostBase, db: Session = Depends(get_db)):
    post = db.query(Post).filter(Post.id == id).first()
    if not post:
        raise HTTPException(status_code=404, detail="Post not found")
    for key, value in updated.model_dump().items():
        setattr(post, key, value)
    db.commit()
    db.refresh(post)
    return post

@router.patch("/{id}", response_model=schemas.PostResponse)
def update_post_patch(id, updated: schemas.PostUpdatePATCH, db: Session = Depends(get_db)):
    post = db.query(Post).filter(Post.id == id).first()
    if not post:
        raise HTTPException(status_code=404, detail="Post not found")
    for key, value in updated.model_dump(exclude_unset=True).items():
        setattr(post, key, value)
    db.commit()
    db.refresh(post)
    return post

@router.delete("/{id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_post(id: int, db: Session = Depends(get_db)):
    post = db.query(Post).filter(Post.id == id).first()
    if not post:
        raise HTTPException(status_code=404, detail="Post not found")
    db.delete(post)
    db.commit()
    return None
